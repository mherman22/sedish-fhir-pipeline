# sedish-fhir-pipeline

[![Build & Test](https://github.com/mherman22/sedish-fhir-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/mherman22/sedish-fhir-pipeline/actions/workflows/ci.yml)

FHIR R4 transformation and load pipeline for the **SEDISH Haiti Health Information Exchange**.
Reads patient demographics and clinical data from the CHARESS consolidated OpenMRS database,
maps it to FHIR R4 using [SQLMesh](https://sqlmesh.com), and delivers it to **OpenCR** (MPI)
and the **Shared Health Record** (SHR) through OpenHIM — continuously, in near-real time.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Continuous Operation](#continuous-operation)
- [Deployment](#deployment)
- [Mapping Reference](#mapping-reference)
- [Testing](#testing)
- [Project Structure](#project-structure)
- [Known Limitations](#known-limitations)

---

## Overview

SEDISH is Haiti's national Health Information Exchange. Multiple iSantePlus EMR sites
(HUEH, HUP, HFSCJ, and others) each maintain their own OpenMRS instance. The CHARESS
*Consolidé* server aggregates all site data into a single `consolidated_db` via MySQL
binlog CDC. **Consolidé is external and read-only to us**, and MySQL can't JOIN across
servers, so SQLMesh can't run there. This pipeline sits between Consolidé and SEDISH:

1. **Sync** — `loader/sync_source.py` copies `consolidated_db` from the read-only Consolidé
   into a **local MySQL** (full on first run, incremental by `date_updated` after), so SQLMesh
   has the source locally. Needs only read-only `SELECT` on Consolidé.
2. **Transform** — SQLMesh models read the local `consolidated_db` copy and emit FHIR R4 JSON
   for each patient (and, in Phase 2, their clinical record), writing to a local `fhir` schema.
3. **Load** — a Python loader computes what changed (per-resource watermarks) and POSTs FHIR
   transaction Bundles to a single OpenHIM channel, `/consolidated/fhir`. It does **not** split
   CR/SHR itself — the [`fhir-router-mediator`](https://github.com/mherman22/fhir-router-mediator)
   routes each bundle by resource type:
   - **Patient → OpenCR** — identity; OpenCR de-duplicates across sites into golden records.
   - **clinical → SHR** (HAPI FHIR) — Encounters, Observations, Conditions, Allergies,
     MedicationRequests, linked to the same patient.

   Identity and clinical run every cycle off their own watermarks: a demographics-only change
   goes to OpenCR only; a clinical change carries the patient (as the SHR's reference target).
   To run identity-only (e.g. before the SHR is validated), set `CLINICAL_VIEWS=` (empty) — just
   config, no redeploy.

This repo covers *sync*, *transform*, and *load*. The *extract* — replicating site data into
Consolidé's `consolidated_db` — is the Consolidé server's responsibility.

---

## Architecture

Full architecture diagram: [SEDISH / Roaming Care architecture](https://www.canva.com/design/DAHK9iq2S7Q/MZ10sWdDlGyRetfetoz8-Q/edit)

```
 Site 1 (iSantePlus) ┐
 Site 2 (iSantePlus) ┤── binlog CDC ──▶  Consolidé: consolidated_db   (EXTERNAL, read-only)
 Site N (iSantePlus) ┘                   (OpenMRS-shaped, multi-site)
                                                    │  ① sync_source.py (read-only SELECT,
                                                    │     incremental by date_updated)
                                                    ▼
                                         local MySQL: consolidated_db copy + fhir output
                                                    │  ② SQLMesh models  consolidated_db → fhir.*
                                                    │  ③ loader/push_to_openhim.py
                                                    │     POST bundles → /consolidated/fhir
                                                    ▼
                                         OpenHIM → fhir-router mediator (splits by type)
                                                                          │
                                                      ┌───────────────────┴────────────────────┐
                                                      ▼                                        ▼
                                             /CR/fhir → OpenCR                    /SHR/fhir → SHR
                                             (MPI: identity, dedup,               (HAPI FHIR: clinical)
                                              golden records)
```

### Why an MPI is necessary

Each iSantePlus site assigns its own patient IDs, so the same person appears under different
MRNs at different facilities. This pipeline does not attempt cross-site resolution; instead,
it attaches every available identifier — per-site MRNs and the national biometric fingerprint
ID — to the FHIR Patient resource and lets OpenCR perform deduplication.

The national fingerprint ID (`HT-…`) is the authoritative cross-site key. Records with
`statut = UNIQUE` carry a unique fingerprint; records with `statut = DOUBLON` share the
canonical ID of another site's record for the same person. OpenCR collapses both into a
single golden record, and the SHR re-points all clinical references onto it.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Python ≥ 3.11 | `uv` recommended for dependency management |
| Consolidé MySQL ≥ 8.0 | **Read-only** `SELECT` on `consolidated_db` (the external source) |
| Local MySQL ≥ 8.0 | Writable — holds the synced `consolidated_db` copy + the `fhir` output/state (`--sql-mode=` for legacy zero-dates) |
| OpenHIM | The `fhir-router` mediator on channel `/consolidated/fhir` (it forwards to `/CR/fhir` + `/SHR/fhir`) |

---

## Installation

```bash
git clone https://github.com/uwdigi/sedish-fhir-pipeline.git
cd sedish-fhir-pipeline
uv sync          # or: pip install -e .
```

---

## Configuration

Copy the configuration template and fill in your connection details:

```bash
cp config.template.yaml config.yaml
```

`config.yaml` is git-ignored. SQLMesh uses **one gateway** — the **local** MySQL, which holds
both the synced `consolidated_db` copy and the `fhir` output (source and output must be on one
server; MySQL can't JOIN across servers):

```yaml
gateways:
  mysql:
    connection:
      type: mysql
      host: <local-mysql-host>          # the local pipeline MySQL (NOT Consolidé)
      database: digi_fhir               # writable output + state schema
default_gateway: mysql
model_defaults: {dialect: mysql}
```

The model variables (`national_id_system`, `source_key_system`, `phone_attribute_name`) all
**self-default** in the models, so no `variables:` block is required.

The **sync** reads Consolidé and the **loader/transform** read+write the local MySQL — set via env:

| Variable | Default | Description |
|---|---|---|
| `SRC_HOST` / `SRC_PORT` / `SRC_USER` / `SRC_PASS` | — | external Consolidé MySQL (read-only) the sync copies from |
| `SRC_DB` | `consolidated_db` | source schema on Consolidé |
| `FHIR_DB_HOST` / `PORT` / `USER` / `PASS` | — | the **local** MySQL SQLMesh reads (synced copy) + writes (`fhir`) |
| `FHIR_DB_NAME` | `fhir` | local output schema |
| `MEDIATOR_URL` | `http://openhim-core:5001/consolidated/fhir` | OpenHIM channel the `fhir-router` mediator serves (it splits CR/SHR) |
| `OPENHIM_USER` / `OPENHIM_PASS` | `consolidated` | OpenHIM client (role `emr`) used for the mediator channel |
| `CLINICAL_VIEWS` | `encounter,observation,allergy_intolerance,condition,medication_request` | patient-scoped views bundled per patient; set empty for identity-only |
| `DRY_RUN` | `0` | `1` = preview changes without writing to OpenHIM |
| `INTERVAL` | `30` | Poll interval in seconds (continuous mode) |

### Two modes (set by whether `SRC_*` is configured)

- **SYNC** (default, `SRC_*` set) — Consolidé is read-only / a separate server. The pipeline syncs
  `consolidated_db` into the local `FHIR_DB` MySQL, then SQLMesh runs there. (No write needed on Consolidé.)
- **DIRECT** (no `SRC_*`) — if we get **write access to Consolidé**, skip the local copy entirely:
  point `FHIR_DB_*` at Consolidé itself (a writable `fhir` schema beside `consolidated_db`), leave
  `SRC_*` unset. SQLMesh reads `consolidated_db` and writes `fhir` on that one server — no sync, no
  local MySQL. Same image; just env. The deploy package would then drop the `pipeline-db` service.

---

## Running the Pipeline

### 1. Sync — copy the read-only source into the local MySQL

```bash
python loader/sync_source.py
```

Copies the tables the models read from the external Consolidé `consolidated_db` into the local
MySQL — full on first run, then incremental by `date_updated` (watermark in `sync_state`). Needs
only read-only `SELECT` on Consolidé.

### 2. Transform — build the FHIR views

```bash
sqlmesh plan --auto-apply
```

Materialises `fhir.patient`, `fhir.encounter`, `fhir.observation`, `fhir.condition`,
`fhir.allergy_intolerance`, `fhir.medication_request`, and `fhir.location`. On subsequent
runs only rows whose `changed_at` watermark has advanced are reprocessed.

### 3. Load — push to OpenCR and SHR

```bash
# Preview first
DRY_RUN=1 python loader/push_to_openhim.py

# Then write
python loader/push_to_openhim.py
```

The loader compares each row's `changed_at` against a per-resource high-water mark in
`loader_state` and POSTs transaction Bundles (PUT-by-id entries) to the mediator only for
records that have changed. Both stages are **idempotent** — re-running is always safe.

### 4. Verify

```bash
# Identity: two site records for one person should share one golden record
curl -su openshr:openshr \
  'http://openhim-core:5001/CR/fhir/Patient?identifier=<national-id>'

# Clinical: confirm resources landed in the SHR
curl -s 'http://hapi-fhir:8080/fhir/Encounter/<uuid>'
```

---

## Continuous Operation

The pipeline runs indefinitely. A new or updated record in `consolidated_db` propagates to
OpenCR and the SHR within one cycle (~5 minutes).

**Poll driver:**

```bash
INTERVAL=30 bash loader/run_continuous.sh
```

Runs **sync → `sqlmesh run` → load → sleep** in a loop (`INTERVAL` seconds). All stages are
idempotent, so a re-run never double-creates — it converges. (Consolidé is external and does not
publish an event stream to us, so the pipeline polls rather than subscribing.)

---

## Deployment

The included `Dockerfile` builds the production image. On start it renders `config.yaml`
from environment variables (`FHIR_DB_HOST/USER/PASS` are required — the Consolidé MySQL),
applies the initial `sqlmesh plan`, then runs the continuous poll loop. The SEDISH
`sedish-fhir-pipeline` instant package builds this image locally (`sedish-fhir-pipeline:local`)
and runs it as the `fhir-pipeline` service.

---

## Mapping Reference

All rules are derived from the CHARESS specification.

| Concern | Rule |
|---|---|
| Composite key | `(mspp_code, patient_id)` — `patient_id` alone is not unique across sites |
| Resource ID | OpenMRS `uuid`; MD5-derived stable key for tables without a uuid |
| Gender | Accepts code (`M`/`F`) and label (`Male`/`Female`) |
| Per-site MRNs | `Patient.identifier` via `ref.identifier_systems` (must match OpenCR's `internalid` config) |
| National fingerprint ID | Attached only for `statut ∈ {UNIQUE, DOUBLON}`; DOUBLON reuses the canonical shared ID |
| Status downgrade | `UNIQUE → A_REVOIR` advances `changed_at` via the `fp_chg` CTE, triggering a re-push to OpenCR even though the national ID is removed |
| Observations | `value[x]` dispatched by type (numeric, coded, datetime, text, drug) |
| Phone | `Patient.telecom` from the `Telephone Number` person attribute (feeds OpenCR's phone match rule) |

See [`examples/`](examples/) for representative FHIR output — Patient (with and without a
national ID), Encounter, Observation, Condition, AllergyIntolerance, MedicationRequest, and a
complete SHR transaction Bundle.

---

## Testing

```bash
sqlmesh test
```

Unit tests live in `tests/<domain>/` as self-contained YAML fixtures (input rows → expected
output rows). Each test declares its own `vars` inline and only the columns the model touches.

---

## Project Structure

```
config.template.yaml        gateway definitions (source + output DB), FHIR variable defaults
external_models.yaml        typed column declarations for consolidated_db source tables
models/fhir/                FHIR R4 mapping models (one .sql per resource type)
models/ref_*/               reference seed data (identifier_type → FHIR system URI)
loader/
  sync_source.py            sync: external read-only consolidated_db -> local copy
  push_to_openhim.py        delta loader — reads fhir.* views, PUTs to OpenHIM
  run_continuous.sh         poll driver (the continuous loop)
audits/                     SQLMesh data quality assertions
tests/<domain>/             unit tests by resource domain
examples/                   representative FHIR JSON output from the models
documentation/domains/      per-resource mapping notes
```

---

## Known Limitations

Verified against the live `consolidated_db` (`54.212.165.76`, 2026-06-16 — a ~522-patient test set):

- **`national_fingerprint_mapping` is present and populated** — 506 rows (400 UNIQUE / 106
  DOUBLON), columns match the model. The identity/DOUBLON + fpnid path runs against real data. ✅
- **`patient_identifier_type` is empty, and every patient identifier is `identifier_type = 5`**
  (e.g. `TST11001001013`). The seed `ref_identifier_systems.csv` assumes `5 = Code National`, but
  with the dimension table empty this can't be confirmed from the DB — **CHARESS must confirm what
  type 5 is** (and 3/6) before go-live, or OpenCR will index identifiers under the wrong system.
- **`person_attribute_openmrs` is empty** — no phone data, so `Patient.telecom` is not emitted
  (OpenCR Rule 10 won't fire). Harmless; telecom is conditional.
- **`patient_isanteplus.mother_name` is 100% populated** — `Patient.contact[MTH]` is emitted for
  every patient (feeds OpenCR Rule 11).
- **No `concept_reference_*` table** — Observations use `concept_name` labels and local concept
  codes. CIEL/SNOMED codings will be added once the reference table is confirmed.
- **No `provider` table** — `MedicationRequest.requester` is omitted (`encounter_provider_openmrs`
  carries per-link UUIDs, not per-provider UUIDs)..
