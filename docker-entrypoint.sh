#!/usr/bin/env sh
# Render config.yaml, sync the external Consolidé source into the local MySQL, build the
# output schema, then serve the continuous loop.
set -e

# FHIR_DB_* = the LOCAL pipeline MySQL SQLMesh reads+writes (source copy + fhir output).
: "${FHIR_DB_HOST:?FHIR_DB_HOST is required (the local pipeline MySQL)}"
: "${FHIR_DB_USER:?FHIR_DB_USER is required}"
: "${FHIR_DB_PASS:?FHIR_DB_PASS is required}"
: "${FHIR_DB_PORT:=3306}"
: "${FHIR_DB_NAME:=fhir}"
: "${FHIR_TEST_DB:=fhir_test}"
# SRC_* = the EXTERNAL Consolidé MySQL (read-only) we sync the source from.
: "${SRC_HOST:?SRC_HOST is required (the external Consolidé MySQL host)}"
: "${SRC_USER:?SRC_USER is required}"
: "${SRC_PASS:?SRC_PASS is required}"
: "${SRC_PORT:=3306}"
: "${SRC_DB:=consolidated_db}"
: "${DST_DB:=consolidated_db}"

cat > /app/config.yaml <<YAML
gateways:
  mysql:
    connection: {type: mysql, host: ${FHIR_DB_HOST}, port: ${FHIR_DB_PORT}, user: ${FHIR_DB_USER}, password: ${FHIR_DB_PASS}, database: ${FHIR_DB_NAME}}
    test_connection: {type: mysql, host: ${FHIR_DB_HOST}, port: ${FHIR_DB_PORT}, user: ${FHIR_DB_USER}, password: ${FHIR_DB_PASS}, database: ${FHIR_TEST_DB}}
default_gateway: mysql
model_defaults: {dialect: mysql}
disable_anonymized_analytics: true
YAML

# Wait for the LOCAL MySQL and ensure the output/state schemas exist. Idempotent.
echo "entrypoint: waiting for local MySQL ${FHIR_DB_HOST} + ensuring schemas"
until python - <<PY 2>/dev/null
import pymysql
c = pymysql.connect(host="${FHIR_DB_HOST}", port=${FHIR_DB_PORT}, user="${FHIR_DB_USER}", password="${FHIR_DB_PASS}")
with c.cursor() as cur:
    cur.execute("CREATE DATABASE IF NOT EXISTS \`${FHIR_DB_NAME}\`")
    cur.execute("CREATE DATABASE IF NOT EXISTS \`${FHIR_TEST_DB}\`")
    cur.execute("CREATE DATABASE IF NOT EXISTS \`${DST_DB}\`")
c.commit()
PY
do
  echo "entrypoint: local MySQL not ready, retrying in 5s"; sleep 5
done

# Initial sync: copy the external (read-only) consolidated_db into the local MySQL, so SQLMesh
# has the source locally (MySQL can't JOIN across servers). Retry until Consolidé is reachable.
echo "entrypoint: initial sync from Consolidé ${SRC_HOST}"
until python loader/sync_source.py; do
  echo "entrypoint: sync not ready (Consolidé unreachable?), retrying in 10s"; sleep 10
done

# Build the output schema from the synced source.
echo "entrypoint: applying initial sqlmesh plan"
until sqlmesh plan --auto-apply --skip-tests; do
  echo "entrypoint: plan failed, retrying in 10s"; sleep 10
done

echo "entrypoint: starting the continuous loop (sync -> transform -> load)"
exec sh loader/run_continuous.sh
