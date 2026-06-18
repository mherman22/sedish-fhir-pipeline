#!/usr/bin/env sh
# Render config.yaml, (optionally) sync the external Consolidé source into the MySQL SQLMesh
# uses, build the output schema, then serve the continuous loop.
#
# Two modes, selected by whether SRC_HOST is set:
#   SYNC mode (SRC_* set)  — Consolidé is read-only / a different server. Sync consolidated_db
#                            into FHIR_DB (a local MySQL); SQLMesh runs there. (default deploy)
#   DIRECT mode (no SRC_*) — we have write access to Consolidé. Point FHIR_DB_* at Consolidé
#                            itself (a writable `fhir` schema beside consolidated_db); SQLMesh
#                            reads consolidated_db + writes fhir on that one server. No sync,
#                            no local copy.
set -e

# FHIR_DB_* = the MySQL SQLMesh reads + writes (local copy in SYNC mode; Consolidé in DIRECT mode).
: "${FHIR_DB_HOST:?FHIR_DB_HOST is required}"
: "${FHIR_DB_USER:?FHIR_DB_USER is required}"
: "${FHIR_DB_PASS:?FHIR_DB_PASS is required}"
: "${FHIR_DB_PORT:=3306}"
: "${FHIR_DB_NAME:=fhir}"
: "${FHIR_TEST_DB:=fhir_test}"   # set empty to omit the test gateway (prod/DIRECT — no `sqlmesh test`)
: "${ENSURE_DBS:=1}"             # set 0 when schemas are pre-created (DIRECT/prod, no CREATE privilege)

# test_connection only when a test DB is configured (CI/dev); omitted in prod so no fhir_test needed.
TEST_CONN=""
if [ -n "${FHIR_TEST_DB}" ]; then
  TEST_CONN="
    test_connection: {type: mysql, host: ${FHIR_DB_HOST}, port: ${FHIR_DB_PORT}, user: ${FHIR_DB_USER}, password: ${FHIR_DB_PASS}, database: ${FHIR_TEST_DB}}"
fi

cat > /app/config.yaml <<YAML
gateways:
  mysql:
    connection: {type: mysql, host: ${FHIR_DB_HOST}, port: ${FHIR_DB_PORT}, user: ${FHIR_DB_USER}, password: ${FHIR_DB_PASS}, database: ${FHIR_DB_NAME}}${TEST_CONN}
default_gateway: mysql
model_defaults: {dialect: mysql}
disable_anonymized_analytics: true
YAML

# Wait for FHIR_DB. With ENSURE_DBS=1 (default) create the schemas; with ENSURE_DBS=0 (DIRECT, schemas
# pre-created by CHARESS and no CREATE privilege) just verify the DB is reachable + exists. Idempotent.
echo "entrypoint: waiting for MySQL ${FHIR_DB_HOST} (ensure_dbs=${ENSURE_DBS})"
until python - <<PY 2>/dev/null
import pymysql
c = pymysql.connect(host="${FHIR_DB_HOST}", port=${FHIR_DB_PORT}, user="${FHIR_DB_USER}", password="${FHIR_DB_PASS}")
with c.cursor() as cur:
    if "${ENSURE_DBS}" == "1":
        cur.execute("CREATE DATABASE IF NOT EXISTS \`${FHIR_DB_NAME}\`")
        if "${FHIR_TEST_DB}":
            cur.execute("CREATE DATABASE IF NOT EXISTS \`${FHIR_TEST_DB}\`")
    cur.execute("USE \`${FHIR_DB_NAME}\`")
c.commit()
PY
do
  echo "entrypoint: MySQL not ready / ${FHIR_DB_NAME} missing, retrying in 5s"; sleep 5
done

# CDC mode (SYNC_MODE=cdc): the binlog reader does its own snapshot + stream + downstream cycles,
# so skip the poll-sync here. Requires SRC_* to point at Consolidé with REPLICATION privileges.
if [ "${SYNC_MODE:-poll}" = "cdc" ]; then
  : "${SRC_HOST:?SRC_HOST is required for SYNC_MODE=cdc}"
  echo "entrypoint: applying initial sqlmesh plan (CDC mode)"
  until sqlmesh plan --auto-apply --skip-tests; do
    echo "entrypoint: plan failed, retrying in 10s"; sleep 10
  done
  echo "entrypoint: starting CDC binlog reader"
  exec python loader/cdc_stream.py
fi

# POLL/DIRECT mode (default). SYNC mode: copy the external (read-only) consolidated_db into FHIR_DB
# so SQLMesh has the source locally (MySQL can't JOIN across servers). Skipped in DIRECT mode.
if [ -n "${SRC_HOST:-}" ]; then
  : "${SRC_USER:?SRC_USER is required when SRC_HOST is set}"
  : "${SRC_PASS:?SRC_PASS is required when SRC_HOST is set}"
  echo "entrypoint: initial sync from Consolidé ${SRC_HOST}"
  until python loader/sync_source.py; do
    echo "entrypoint: sync not ready (Consolidé unreachable?), retrying in 10s"; sleep 10
  done
else
  echo "entrypoint: no SRC_HOST — DIRECT mode (consolidated_db must live on ${FHIR_DB_HOST})"
fi

# Build the output schema.
echo "entrypoint: applying initial sqlmesh plan"
until sqlmesh plan --auto-apply --skip-tests; do
  echo "entrypoint: plan failed, retrying in 10s"; sleep 10
done

echo "entrypoint: starting the continuous loop"
exec sh loader/run_continuous.sh
