# sedish-fhir-pipeline

[![Build & Test](https://github.com/DIGI-UW/sedish-fhir-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/DIGI-UW/sedish-fhir-pipeline/actions/workflows/ci.yml)
[![Publish image](https://github.com/DIGI-UW/sedish-fhir-pipeline/actions/workflows/publish.yml/badge.svg)](https://github.com/DIGI-UW/sedish-fhir-pipeline/actions/workflows/publish.yml)

Maps the CHARESS consolidated OpenMRS database to FHIR R4 with [SQLMesh](https://sqlmesh.com) and
delivers it to **OpenCR** (the MPI) and the **Shared Health Record (SHR)** through OpenHIM —
continuously, in near-real-time. Part of the **SEDISH** Haiti Health Information Exchange.

## What it does

The CHARESS *Consolidé* server aggregates every iSantePlus site into one `consolidated_db`. This
pipeline turns that into FHIR and pushes it into the HIE:

1. **Transform** — SQLMesh models read `consolidated_db` and build a `fhir` schema, one model per
   resource type: `patient`, `encounter`, `visit`, `observation`, `condition`, `allergy_intolerance`,
   `medication_request`, `location`. (`visit` is an OpenMRS Visit mapped to a FHIR `Encounter`,
   tagged `visit`; each `encounter` links up to its visit via `Encounter.partOf`.)
2. **Load** — `loader/push_to_openhim.py` works out what changed and POSTs per-patient FHIR
   transaction Bundles to a single OpenHIM channel, `/consolidated/fhir`. It does **not** know the
   CR/SHR split — the [fhir-router mediator](https://github.com/DIGI-UW/fhir-router-mediator) routes
   each bundle: **Patient → OpenCR** (identity; OpenCR dedupes the sites into golden records) and
   **clinical → SHR** (HAPI FHIR).

It runs as a continuous loop, so a new or edited record in `consolidated_db` reaches OpenCR and the
SHR within one cycle (default 30s).

## How we run it — SYNC mode

We have **read-only** access to Consolidé, and MySQL can't JOIN across servers, so SQLMesh can't
transform there in place. So in the mode we run:

- A **local MySQL** holds a synced copy of `consolidated_db`, and SQLMesh transforms against that copy.
- `loader/sync_source.py` refreshes the copy each cycle with **per-entry change detection**: for
  every source row it compares `(primary key, GREATEST(date_updated, date_changed, date_created))`
  against the local copy and re-copies only new/changed rows (and drops rows gone from the source).
  The local copy *is* the processed-state — no separate watermark table — so an edit is caught
  whenever it arrives, not just inside a "since last run" window. Reference tables with no timestamp
  are copied once and cached.

> A **DIRECT** mode also exists (SQLMesh runs on Consolidé itself, no copy) for when write access is
> available — point `FHIR_DB_*` at Consolidé and leave `SRC_*` unset. We don't use it today.

## The loop

`loader/run_continuous.sh`, every `INTERVAL` (default 30s):

1. `sync_source.py` — copy changed rows into the local DB (SYNC only)
2. `sqlmesh run` — incrementally rebuild `fhir.*` for changed rows
3. `push_to_openhim.py` — POST changed per-patient bundles to the mediator
4. `reconcile.py` — retract SHR clinical the source no longer produces (off unless `RECONCILE_RETRACT_EVERY > 0`)

Every stage is idempotent (sync REPLACEs by PK, SQLMesh tracks its high-water mark, the loader PUTs
by id and holds its watermark on failure), so a failed or repeated cycle converges instead of
duplicating.

## Configuration

The container renders `config.yaml` from environment variables on start. Main ones:

| Variable | Default | Purpose |
|---|---|---|
| `FHIR_DB_HOST/PORT/USER/PASS` | — | MySQL SQLMesh reads + writes (the **local** copy in SYNC) |
| `FHIR_DB_NAME` | `fhir` | output schema |
| `SRC_HOST/PORT/USER/PASS` | — | read-only Consolidé to sync from (**SYNC**; unset ⇒ DIRECT) |
| `MEDIATOR_URL` | `http://openhim-core:5001/consolidated/fhir` | the fhir-router channel |
| `OPENHIM_USER/PASS` | `consolidated` | OpenHIM client (role `emr`) for that channel |
| `CLINICAL_VIEWS` | `encounter,visit,observation,allergy_intolerance,condition,medication_request` | per-patient clinical bundled; empty = identity-only |
| `RECONCILE_RETRACT_EVERY` | `0` | seconds between SHR retraction passes; `0` = off |
| `INTERVAL` | `30` | seconds between cycles |
| `DRY_RUN` | `0` | `1` = preview, no writes to OpenHIM |

## Run locally

```bash
uv sync                                 # install deps
cp config.template.yaml config.yaml     # fill in your connections
python loader/sync_source.py            # SYNC: seed the local copy
sqlmesh plan --auto-apply               # build fhir.*
DRY_RUN=1 python loader/push_to_openhim.py   # preview the load
python loader/push_to_openhim.py             # push for real
sqlmesh test                            # unit tests
```

## Deployment

The `Dockerfile` builds the production image: it renders `config.yaml` from env, seeds the local
copy (SYNC), applies the initial `sqlmesh plan`, then runs `run_continuous.sh`. CI publishes
`ghcr.io/digi-uw/sedish-fhir-pipeline:main`.

### In SEDISH (the `data-pipeline-consolidated-server` package)

It's deployed by the SEDISH `data-pipeline-consolidated-server` instant package as the
`fhir-pipeline` service. `.env` (SYNC default — `CONSOLIDATED_*` is the read-only Consolidé user;
the compose maps it to `SRC_*`/`FHIR_DB_*`):

```bash
CONSOLIDATED_HOST=<consolidé-host>
CONSOLIDATED_USER=<read-only-user>
CONSOLIDATED_PASS=<password>
PIPELINE_DB_PW=pipeline                       # local pipeline-db root password
PIPELINE_IMAGE=ghcr.io/digi-uw/sedish-fhir-pipeline:main
```

```bash
./build-image.sh
./instant package init -n data-pipeline-consolidated-server --env-file .env
```

For DIRECT mode (write access on Consolidé), CHARESS pre-creates the `fhir`/`sqlmesh`/`sqlmesh__fhir`
schemas + grants, then swap in `docker-compose.direct.yml`. Full deploy/verify/troubleshooting:
the SEDISH repo's `docs/consolidated-pipeline-setup.md`.

## Project structure

```
models/fhir/                 FHIR R4 mapping models (one .sql per resource type)
loader/sync_source.py        SYNC — per-entry copy of consolidated_db into the local DB
loader/push_to_openhim.py    delta loader — fhir.* → per-patient bundles → mediator
loader/reconcile.py          retract SHR clinical no longer present in the source
loader/run_continuous.sh     the loop (sync → run → load → reconcile → sleep)
tests/<domain>/              SQLMesh unit tests (YAML input → expected output)
examples/                    representative FHIR JSON from the models
```

## Status & known gaps

Verified end-to-end against the live `consolidated_db` (~522-patient test set): demographics and
identity (including the fingerprint UNIQUE/DOUBLON → golden-record path) and clinical data land
correctly. Open items, pending CHARESS confirmation:

- `patient_identifier_type` dimension is empty in the consolidated extract — the table exists but
  carries no rows. The iSantePlus init dump confirms the standard ID assignments (3 = iSantePlus
  ID, 4 = Code ST, 5 = Code National, 6 = Biometrics NRC); the identifier seed has been updated
  to match. CHARESS should run `documentation/verify-identifier-types.sql` against the live
  consolidated_db to confirm those IDs hold, then populate the dimension so the ETL has labels
  to stamp.
- No concept reference tables in the consolidated extract yet — `concept_reference_map`,
  `concept_reference_term`, and `concept_reference_source` exist in the iSantePlus schema and
  carry CIEL/LOINC/SNOMED mappings; `concept_numeric` carries UCUM units. Once CHARESS adds
  these to the consolidated extract, secondary codings and `valueQuantity.unit` can be emitted.
- Phone and provider data are absent in the test set, so `Patient.telecom` is conditionally
  omitted; `Encounter.participant` requires the `provider` table (present in iSantePlus but not
  yet in the consolidated extract).
- `visit` (Visit-as-`Encounter`) omits `Encounter.type` — the consolidated extract has no
  visit-type label table (`visit_openmrs.visit_type_id` is present but there's nothing to
  resolve it to a name). It can be added once CHARESS includes a visit-type dimension.
