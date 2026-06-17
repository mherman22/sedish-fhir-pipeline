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
: "${FHIR_TEST_DB:=fhir_test}"

cat > /app/config.yaml <<YAML
gateways:
  mysql:
    connection: {type: mysql, host: ${FHIR_DB_HOST}, port: ${FHIR_DB_PORT}, user: ${FHIR_DB_USER}, password: ${FHIR_DB_PASS}, database: ${FHIR_DB_NAME}}
    test_connection: {type: mysql, host: ${FHIR_DB_HOST}, port: ${FHIR_DB_PORT}, user: ${FHIR_DB_USER}, password: ${FHIR_DB_PASS}, database: ${FHIR_TEST_DB}}
default_gateway: mysql
model_defaults: {dialect: mysql}
disable_anonymized_analytics: true
YAML

# Wait for FHIR_DB and ensure the output/state schemas exist. Idempotent.
echo "entrypoint: waiting for MySQL ${FHIR_DB_HOST} + ensuring ${FHIR_DB_NAME}/${FHIR_TEST_DB}"
until python - <<PY 2>/dev/null
import pymysql
c = pymysql.connect(host="${FHIR_DB_HOST}", port=${FHIR_DB_PORT}, user="${FHIR_DB_USER}", password="${FHIR_DB_PASS}")
with c.cursor() as cur:
    cur.execute("CREATE DATABASE IF NOT EXISTS \`${FHIR_DB_NAME}\`")
    cur.execute("CREATE DATABASE IF NOT EXISTS \`${FHIR_TEST_DB}\`")
c.commit()
PY
do
  echo "entrypoint: MySQL not ready, retrying in 5s"; sleep 5
done

# SYNC mode: copy the external (read-only) consolidated_db into FHIR_DB so SQLMesh has the source
# locally (MySQL can't JOIN across servers). Skipped in DIRECT mode (consolidated_db already on FHIR_DB).
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
