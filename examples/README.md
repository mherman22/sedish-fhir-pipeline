# FHIR output examples

These are the **actual FHIR resources the SQLMesh models emit** — the `resource` column of
each `fhir.*` model, dumped from a run against a representative `consolidated_db` dataset (one
site, `mspp_code 11106`, patient `Jean Baptiste`). They show what `consolidated_db` rows turn
into before the loader sends them to OpenCR (`/CR/fhir`) and the SHR (`/SHR/fhir`).

Every resource is shaped to match the **OpenMRS fhir2 module** output, so SHR records reconcile
with what the EMRs themselves produce.

| File | What it shows |
|------|----------------|
| `patient.json` | A patient **with** the national fingerprint id (statut `UNIQUE`/`DOUBLON`). Two identifiers: the per-site MRN and the national id — the latter is what lets OpenCR link this person across facilities. fhir2 shape: element `id`s on name/address/identifier, `type.text` from the identifier-systems ref, the `ext/address` extension, and `deceasedBoolean`. |
| `patient-no-national-id.json` | A patient **without** a national id (statut `A_REVOIR`). Same shape, but `identifier` has only the per-site MRN — so OpenCR can't cross-link it. |
| `encounter.json` | An Encounter; two `meta.tag`s (the fhir2 `encounter-tag` distinguishing it from a Visit, plus the originating `mspp-site`), `subject` by patient uuid, `period.start` as a T-separated FHIR `dateTime`. |
| `observation.json` | A numeric Observation: `code` is the **concept UUID with no system** (fhir2), `valueQuantity`, `subject` (+`type`), `encounter` reference. |
| `allergy-intolerance.json` | An AllergyIntolerance matching the fhir2 `AllergyIntoleranceTranslator`: coded allergen by concept UUID, hardcoded `type`, `clinicalStatus`/`verificationStatus` (coding + display + text), `category`, `reaction.manifestation`, and a `note`. |
| `condition.json` | A Condition from the derived `patient_diagnosis` table: synthetic `id` (MD5 of the natural key, no source uuid), diagnosis `code` by concept UUID, `clinicalStatus`, `encounter-diagnosis` category, `subject` (+`type`). |
| `medication-request.json` | A MedicationRequest from the derived `patient_prescription` table: synthetic `id`, `subject`/`requester` (+`type`), `dosageInstruction` (text + `boundsDuration`). Medication stays a bare `drug` code (no drug dictionary captured yet). |
| `location.json` | A global Location reference: `name`, `status`, address detail in the fhir2 `ext/address` extension. |
| `shr-transaction-bundle.json` | The transaction `Bundle` the loader POSTs to `/SHR/fhir` for one patient — the Patient plus its Encounters/Observations/Allergies/Conditions/MedicationRequests, each as a `PUT` entry keyed by `resourceType/id`. |

## How the mapping works (source → FHIR)
- **id** = the OpenMRS `uuid` where the source has one (Patient, Encounter, Observation,
  AllergyIntolerance). The derived tables (`patient_diagnosis`, `patient_prescription`) have no
  uuid, so Condition/MedicationRequest use a deterministic synthetic id (`MD5` of the natural key).
- **Concept coding follows fhir2**: the primary coding is the **concept UUID with no `system`**.
  When the CIEL/concept dictionary isn't loaded we derive the OpenMRS legacy UUID =
  `concept_id` right-padded with `A` to 36 chars (e.g. `5089` → `5089AAA…`), so codings still
  reconcile with the EMR's fhir2 output. A non-coded value becomes `text` only.
- **Patient.identifier** = per-site MRNs via `ref.identifier_systems` (system URIs must match
  OpenCR's `internalid`), plus the national fingerprint id for `statut ∈ {UNIQUE, DOUBLON}` only.
- **gender** accepts code or label (`M`/`Male`→`male`, `F`/`Female`→`female`).
- **Observation.value[x]** is chosen by the populated `value_*` column (numeric / coded / datetime / text).
- **meta.tag** carries the originating facility (`mspp-site`) on every resource for provenance.
- References (`subject`, `encounter`) point at uuids. The **SHR re-points** `subject` onto OpenCR's
  golden record after matching, so cross-facility data for one person unifies.

> Note: the models also emit `changed_at` / `patient_fhir_id` *columns* used for incremental
> loading — those are pipeline metadata, **not** part of the FHIR `resource` shown here.

## Regenerate
```bash
sqlmesh plan --auto-apply
sqlmesh fetchdf "SELECT resource FROM fhir.patient"   # raw FHIR JSON, one row per resource
```
