# CDC / event-driven integration plan

Status: **planned, partially built.** The current production path is the DIRECT poll (`sqlmesh run`
→ `push_to_openhim.py` every cycle). This document describes moving change-detection to a Kafka
event stream, what's already done on our side, and exactly what we need from CHARESS to finish.

## Why
The poll detects changes via `COALESCE(date_updated, date_changed, date_created)`:
- **New patients** are caught (via `date_created` on insert). ✅
- **Updates** are caught only if `date_updated`/`date_changed` advances in `consolidated_db` — in the
  test data those are NULL, so edits can be missed.
- **Deletes** are never caught.
- At scale, each `sqlmesh run` re-scans `consolidated_db` (read load on CHARESS's prod DB).

CDC fixes all four: it reacts to every INSERT/UPDATE/DELETE regardless of timestamp columns, and
only touches what changed.

## The layering (important)
CHARESS's Debezium reads the **EMR binlogs** (the source) to populate `consolidated_db`. So:

```
 EMR sites ──Debezium (CHARESS)──▶ Kafka (raw EMR change topics) ──▶ sink ──▶ consolidated_db ──▶ [us]
```

`consolidated_db` is the **sink** — the end of *their* CDC and the start of *ours*. Consequences:
- There is likely **no existing stream of `consolidated_db` changes** to subscribe to; the existing
  topics are raw, per-site, **pre-consolidation**.
- We must **not** consume those raw EMR topics — `consolidated_db` denormalizes/derives/reconciles,
  so we'd be re-implementing CHARESS's consolidation and would drift from the actual data.

So a **`consolidated_db`-level** change stream has to be created. Two options:

| Option | Who runs Debezium | Needs from CHARESS | Our extra work |
|--------|-------------------|--------------------|----------------|
| **A (preferred)** | CHARESS, a connector **on `consolidated_db`** | a topic + creds (they already hold replication on their own DB) | a Kafka consumer only |
| **B** | us | `REPLICATION SLAVE, REPLICATION CLIENT` on `consolidated_db` | Kafka Connect + Debezium connector + consumer |

Either way **no copy** and SQLMesh still transforms **in place** (DIRECT).

## Our side

### Already built (tested)
`loader/push_to_openhim.py` → **`push_patients(keys)`**: given a set of patient keys, POST each
patient's FULL current bundle (patient + all clinical) to the mediator, regardless of watermark.
Idempotent (PUT by id), no offset of its own (the caller owns it). This is the key-driven push the
consumer calls per change. Covered by unit tests.

### To build once we know the stream shape
A `loader/run_kafka.py` consumer (thin):
```
consume a batch of Debezium change events
  └─ parse op + table + after/before  →  resolve each to a PATIENT key (mspp_code, patient_id)
  └─ collect distinct changed keys (debounce a few seconds)
sqlmesh run                       # refresh fhir.* in place on Consolidé
push_patients(changed_keys)       # already built
consumer.commit()                 # advance the Kafka offset ONLY after the pushes succeed
```
- **Offset = resume safety.** Commit-after-success → at-least-once; reprocessing is harmless
  (PUT-by-id is idempotent). No watermark table in this mode — Kafka's offset is the watermark.
- The **parser** (topic names → patient key) is the only config-dependent part; it's deliberately
  left until we have their actual topic/envelope shape, to avoid guessing.

## What we need from CHARESS to finish
1. A **`consolidated_db`-level** change stream (Option A: they run the connector; or Option B: grant
   us `REPLICATION` so we run it).
2. **Broker addresses**, **topic naming**, and **auth** (SASL/TLS creds).
3. **Envelope format** — raw Debezium (`{op, before, after, source}`) or unwrapped.
4. Which tables are streamed, and **whether the clinical topics carry the patient key**
   (`person_id`/`patient_id`) so we can resolve each change to a patient.
5. **Delete semantics** — what should an `op:d` do downstream? OpenCR/SHR don't trivially delete
   (void? ignore? flag?). This is a governance decision, not just code.

## Fallback
If neither option lands, we stay on the DIRECT poll. To close the update/delete gap there without
CDC, the cheap re-add is a periodic full re-scan (reconcile) — at the cost of read load on
`consolidated_db`.
