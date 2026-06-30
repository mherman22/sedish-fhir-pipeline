MODEL (
  name fhir.medication_request,
  kind INCREMENTAL_BY_UNIQUE_KEY (unique_key fhir_id),
  cron '*/5 * * * *',
  allow_partials true,
  start '2026-01-01',
  grain (fhir_id),
  columns (
    mspp_code VARCHAR(10),
    patient_id INT,
    fhir_id VARCHAR(39),
    patient_fhir_id VARCHAR(36),
    changed_at DATETIME,
    resource JSON
  ),
  audits (not_null(columns := (mspp_code, fhir_id)))
);

/*
  patient_prescription (iSantePlus DERIVED) -> FHIR MedicationRequest. No uuid, so the id
  is a stable MD5 over the natural PK (encounter_id, location_id, drug_id, mspp_code). LOW
  FIDELITY: drug_id and provider_id are ids only (no drug/provider name tables captured in
  consolidated_db) — medication is a bare code, requester a bare Practitioner reference.
  dosage from posology + number_day. Upgrade once drug/provider reference data is captured.

  priority + encounter follow the OpenMRS fhir2 runtime (MedicationRequestTranslator) and are
  both required (1..1) by the IG omrs-medication-request profile. priority has no source column
  (patient_prescription carries no urgency), so it defaults to 'routine' — the FHIR default and
  the runtime's value for a routine order. encounter resolves pp.encounter_id to the encounter
  uuid; the encounter lands in the same per-patient bundle, so the reference resolves. encounter
  is omitted (no breakage) when the prescription has no encounter.
*/
SELECT
  pp.mspp_code,
  pp.patient_id,
  CONCAT('medreq-', MD5(CONCAT_WS('|', pp.mspp_code, pp.encounter_id, pp.location_id, pp.drug_id))) AS fhir_id,
  @FHIR_ID(per.uuid) AS patient_fhir_id,
  COALESCE(pp.date_updated, pp.last_updated_date, '1970-01-01 00:00:00') AS changed_at,
  JSON_MERGE_PATCH(
    JSON_OBJECT(
      'resourceType', 'MedicationRequest',
      'id', CONCAT('medreq-', MD5(CONCAT_WS('|', pp.mspp_code, pp.encounter_id, pp.location_id, pp.drug_id))),
      'meta', JSON_OBJECT('tag', JSON_ARRAY(JSON_OBJECT(
                'system', @VAR('mspp_site_system', 'http://sedish-haiti.org/fhir/mspp-site'), 'code', pp.mspp_code))),
      'status', 'active',
      'intent', 'order',
      'priority', 'routine',
      'subject', JSON_OBJECT('reference', CONCAT('Patient/', @FHIR_ID(per.uuid)), 'type', 'Patient'),
      'authoredOn', REPLACE(CAST(COALESCE(pp.visit_date, pp.dispensation_date) AS CHAR), ' ', 'T'),
      'medicationCodeableConcept', JSON_OBJECT('coding', JSON_ARRAY(JSON_OBJECT(
                'system', @VAR('drug_system', 'http://isanteplus.org/openmrs/drug'), 'code', CAST(pp.drug_id AS CHAR)))),
      'dosageInstruction', JSON_ARRAY(JSON_OBJECT(
                'text', pp.posology,
                'timing', JSON_OBJECT('repeat', JSON_OBJECT('boundsDuration', JSON_OBJECT(
                  'value', pp.number_day, 'unit', 'd',
                  'system', 'http://unitsofmeasure.org', 'code', 'd')))))
    ),
    CASE WHEN enc.uuid IS NOT NULL
         THEN JSON_OBJECT('encounter', JSON_OBJECT('reference', CONCAT('Encounter/', @FHIR_ID(enc.uuid))))
         ELSE JSON_OBJECT() END
  ) AS resource
FROM consolidated_db.patient_prescription pp
JOIN consolidated_db.person_openmrs per
  ON per.mspp_code = pp.mspp_code AND per.person_id = pp.patient_id
LEFT JOIN consolidated_db.encounter_openmrs enc
  ON enc.mspp_code = pp.mspp_code AND enc.encounter_id = pp.encounter_id
WHERE COALESCE(pp.voided, 0) = 0
