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

SELECT
  e.mspp_code,
  e.encounter_id,
  @FHIR_ID(e.uuid) AS fhir_id,
  @FHIR_ID(per.uuid) AS patient_fhir_id,
  COALESCE(e.date_updated, e.date_created, '1970-01-01 00:00:00') AS changed_at,
  JSON_MERGE_PATCH(
    JSON_OBJECT(
      'resourceType', 'Encounter',
      'id', @FHIR_ID(e.uuid),
      'meta', JSON_OBJECT('tag', JSON_ARRAY(
                JSON_OBJECT('system', 'http://fhir.openmrs.org/ext/encounter-tag',
                            'code', 'encounter', 'display', 'Encounter'),
                JSON_OBJECT('system', @VAR('mspp_site_system', 'http://sedish-haiti.org/fhir/mspp-site'), 'code', e.mspp_code))),
      'status', 'finished',
      'class', JSON_OBJECT('system', 'http://terminology.hl7.org/CodeSystem/v3-ActCode', 'code', 'AMB'),
      'subject', JSON_OBJECT('reference', CONCAT('Patient/', @FHIR_ID(per.uuid)), 'type', 'Patient'),
      'period', JSON_OBJECT('start', REPLACE(CAST(e.encounter_datetime AS CHAR),' ','T'))
    ),
    CASE WHEN et.uuid IS NOT NULL
         THEN JSON_OBJECT('type', JSON_ARRAY(JSON_OBJECT(
                'coding', JSON_ARRAY(JSON_OBJECT('code', et.uuid, 'display', et.name)),
                'text', et.name)))
         ELSE JSON_OBJECT() END
  ) AS resource
FROM consolidated_db.encounter_openmrs e
JOIN consolidated_db.person_openmrs per
  ON per.mspp_code = e.mspp_code AND per.person_id = e.patient_id
LEFT JOIN consolidated_db.encounter_type et
  ON et.encounter_type_id = e.encounter_type AND et.mspp_code = e.mspp_code
WHERE COALESCE(e.voided, 0) = 0
