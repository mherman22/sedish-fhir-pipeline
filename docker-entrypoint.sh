#!/usr/bin/env sh
#
# Entrypoint for the SEDISH FHIR pipeline.
#
# Renders the SQLMesh config from the environment, prepares the database, builds the FHIR
# output schema, and hands off to the continuous loop. Supports two deployment modes, selected
# automatically by whether SRC_HOST is set:
#
#   SYNC mode   (SRC_* set)   — Consolidé is read-only to us. Each cycle copies the changed rows
#                               of consolidated_db into a local MySQL (FHIR_DB_*), where SQLMesh
#                               then transforms them. Needs only SELECT on Consolidé.
#
#   DIRECT mode (SRC_* unset) — we have write access on Consolidé. FHIR_DB_* points at Consolidé
#                               itself; SQLMesh reads consolidated_db and writes the fhir schema
#                               on that one server. No copy, no local MySQL.
#
# The same image serves both; only the environment differs.
#
set -eu

# ── Configuration ────────────────────────────────────────────────────────────
# FHIR_DB_* — the MySQL SQLMesh reads and writes (the local copy in SYNC, Consolidé in DIRECT).
: "${FHIR_DB_HOST:?FHIR_DB_HOST is required}"
: "${FHIR_DB_USER:?FHIR_DB_USER is required}"
: "${FHIR_DB_PASS:?FHIR_DB_PASS is required}"
: "${FHIR_DB_PORT:=3306}"
: "${FHIR_DB_NAME:=fhir}"
: "${FHIR_TEST_DB:=fhir_test}"   # set empty to omit the test gateway (production)
: "${ENSURE_DBS:=1}"             # set 0 when the schemas are pre-created and we lack CREATE

# FHIR system URIs — override to adapt to a different source system or country deployment.
: "${NATIONAL_ID_SYSTEM:=http://isanteplus.org/openmrs/fhir2/6-biometrics-national-reference-code}"
: "${SOURCE_KEY_SYSTEM:=http://sedish-haiti.org/fhir/source-key}"
: "${MSPP_SITE_SYSTEM:=http://sedish-haiti.org/fhir/mspp-site}"
: "${DRUG_SYSTEM:=http://isanteplus.org/openmrs/drug}"
: "${PHONE_ATTRIBUTE_NAME:=Telephone Number}"

MODE=$([ -n "${SRC_HOST:-}" ] && echo SYNC || echo DIRECT)
log() { echo "entrypoint[$MODE]: $*"; }

# ── Render SQLMesh config ────────────────────────────────────────────────────
# The test gateway is only needed for `sqlmesh test` (CI/dev); omit it in production.
TEST_GATEWAY=""
if [ -n "$FHIR_TEST_DB" ]; then
  TEST_GATEWAY="
    test_connection: {type: mysql, host: $FHIR_DB_HOST, port: $FHIR_DB_PORT, user: $FHIR_DB_USER, password: $FHIR_DB_PASS, database: $FHIR_TEST_DB}"
fi

cat > /app/config.yaml <<YAML
gateways:
  mysql:
    connection: {type: mysql, host: $FHIR_DB_HOST, port: $FHIR_DB_PORT, user: $FHIR_DB_USER, password: $FHIR_DB_PASS, database: $FHIR_DB_NAME}$TEST_GATEWAY
default_gateway: mysql
model_defaults: {dialect: mysql}
disable_anonymized_analytics: true
variables:
  national_id_system: $NATIONAL_ID_SYSTEM
  source_key_system: $SOURCE_KEY_SYSTEM
  mspp_site_system: $MSPP_SITE_SYSTEM
  drug_system: $DRUG_SYSTEM
  phone_attribute_name: "$PHONE_ATTRIBUTE_NAME"
YAML

# ── Wait for the database, optionally creating the schemas ───────────────────
# ENSURE_DBS=1 creates fhir (+ fhir_test); ENSURE_DBS=0 only verifies the DB is reachable and the
# output schema exists (pre-created, e.g. DIRECT on Consolidé without CREATE privilege).
log "waiting for MySQL $FHIR_DB_HOST (ensure_dbs=$ENSURE_DBS)"
until python - <<PY 2>/dev/null
import pymysql
conn = pymysql.connect(host="$FHIR_DB_HOST", port=$FHIR_DB_PORT, user="$FHIR_DB_USER", password="$FHIR_DB_PASS")
with conn.cursor() as cur:
    if "$ENSURE_DBS" == "1":
        cur.execute("CREATE DATABASE IF NOT EXISTS \`$FHIR_DB_NAME\`")
        if "$FHIR_TEST_DB":
            cur.execute("CREATE DATABASE IF NOT EXISTS \`$FHIR_TEST_DB\`")
    cur.execute("USE \`$FHIR_DB_NAME\`")
conn.commit()
PY
do
  log "MySQL not ready / $FHIR_DB_NAME missing — retrying in 5s"; sleep 5
done

# ── SYNC mode: seed the local copy from Consolidé before the first transform ──
if [ "$MODE" = "SYNC" ]; then
  : "${SRC_USER:?SRC_USER is required in SYNC mode}"
  : "${SRC_PASS:?SRC_PASS is required in SYNC mode}"
  log "initial sync from Consolidé $SRC_HOST"
  # sync_source.py exits 0 (changes applied) or 20 (clean run, nothing changed) on success;
  # any other code is a real failure (e.g. Consolidé unreachable) -> retry.
  while true; do
    python loader/sync_source.py && break
    rc=$?
    [ "$rc" -eq 20 ] && break
    log "Consolidé not reachable — retrying in 10s"; sleep 10
  done
fi

# ── Build the FHIR output schema ─────────────────────────────────────────────
log "applying initial SQLMesh plan"
until sqlmesh plan --auto-apply --skip-tests; do
  log "plan failed — retrying in 10s"; sleep 10
done

# ── Serve ────────────────────────────────────────────────────────────────────
log "starting the continuous loop"
exec sh loader/run_continuous.sh
