#!/usr/bin/env bash
# Continuous micro-batch: keep consolidated_db → OpenCR/SHR in sync in near-real-time.
#
# This pipeline is NOT a one-time run. A patient changed on Consolidé must flow into SEDISH on
# its own. Each cycle:
#
#   forever:
#     1. sync_source.py       -> pull changed rows (by date_updated) from the external Consolidé
#                                into the local consolidated_db copy (skipped if SRC_HOST unset)
#     2. sqlmesh run          -> incrementally refresh fhir.* for rows changed since last cycle
#     3. push_to_openhim.py   -> upsert new/changed Patients to OpenCR (Phase 2: clinical -> SHR)
#     4. sleep INTERVAL
#
# Latency ≈ INTERVAL. All stages are idempotent (sync REPLACEs by PK, SQLMesh tracks its
# high-water mark, the loader upserts by source key), so a re-run never double-creates.
set -uo pipefail
INTERVAL="${INTERVAL:-30}"   # seconds between cycles
echo "continuous loader: cycle every ${INTERVAL}s (Ctrl-C to stop)"
while true; do
  [ -n "${SRC_HOST:-}" ] && { python loader/sync_source.py || echo "$(date -u +%T) sync failed — retry next cycle"; }
  sqlmesh run                      || echo "$(date -u +%T) sqlmesh run failed — retry next cycle"
  python loader/push_to_openhim.py || echo "$(date -u +%T) load failed — retry next cycle"
  sleep "$INTERVAL"
done
