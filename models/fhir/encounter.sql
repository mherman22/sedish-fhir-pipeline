MODEL (
  name fhir.encounter,
  kind INCREMENTAL_BY_UNIQUE_KEY (unique_key fhir_id),
  cron '*/5 * * * *',
  allow_partials true,
  start '2026-01-01',
  grain (mspp_code, encounter_id),
  columns (
    mspp_code VARCHAR(10),
    encounter_id INT,
    fhir_id VARCHAR(36),
    patient_fhir_id VARCHAR(36),
    changed_at DATETIME,
    resource JSON
  ),
  audits (not_null(columns := (mspp_code, encounter_id, fhir_id)))
);

/* encounter_openmrs -> FHIR Encounter; subject references the patient (person uuid).
   Merged by uuid; `changed_at` = the encounter's consolidated-server write time;
   `patient_fhir_id` lets the loader attach this to its patient's bundle. */
SELECT
  e.mspp_code,
  e.encounter_id,
  e.uuid AS fhir_id,
  per.uuid AS patient_fhir_id,
  COALESCE(e.date_updated, e.date_created, '1970-01-01 00:00:00') AS changed_at,
  JSON_OBJECT(
    'resourceType', 'Encounter',
    'id', e.uuid,
    'meta', JSON_OBJECT('tag', JSON_ARRAY(
              JSON_OBJECT('system', 'http://fhir.openmrs.org/ext/encounter-tag',
                          'code', 'encounter', 'display', 'Encounter'),
              JSON_OBJECT('system', 'http://sedish-haiti.org/fhir/mspp-site', 'code', e.mspp_code))),
    'status', 'finished',
    'class', JSON_OBJECT('system', 'http://terminology.hl7.org/CodeSystem/v3-ActCode', 'code', 'AMB'),
    'subject', JSON_OBJECT('reference', CONCAT('Patient/', per.uuid), 'type', 'Patient'),
    'period', JSON_OBJECT('start', REPLACE(CAST(e.encounter_datetime AS CHAR),' ','T'))
  ) AS resource
FROM consolidated_db.encounter_openmrs e
JOIN consolidated_db.person_openmrs per
  ON per.mspp_code = e.mspp_code AND per.person_id = e.patient_id
WHERE COALESCE(e.voided, 0) = 0
