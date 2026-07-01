MODEL (
  name fhir.condition,
  kind INCREMENTAL_BY_UNIQUE_KEY (unique_key fhir_id),
  cron '*/5 * * * *',
  allow_partials true,
  start '2026-01-01',
  grain (fhir_id),
  columns (
    mspp_code VARCHAR(10),
    patient_id INT,
    fhir_id VARCHAR(37),
    patient_fhir_id VARCHAR(36),
    changed_at DATETIME,
    resource JSON
  ),
  audits (not_null(columns := (mspp_code, fhir_id)))
);

/*
  patient_diagnosis (iSantePlus DERIVED) -> FHIR Condition. clinicalStatus / verificationStatus /
  category are kept to match the OpenMRS fhir2 runtime ConditionTranslator (it emits all three) —
  the OpenMRS FHIR IG profile prohibits them, but we mirror the runtime output the EMRs actually
  send so SHR records reconcile, and we don't stamp meta.profile. Condition.encounter is NOT emitted:
  the fhir2 runtime doesn't set it and the IG omrs-Condition profile prohibits it (Condition.encounter
  0..0), so emitting it would diverge from both. No uuid in the source -> the id is a stable MD5 over
  the natural key.
*/
SELECT
  pd.mspp_code,
  pd.patient_id,
  CONCAT('cond-', MD5(CONCAT_WS('|', pd.mspp_code, pd.encounter_id, pd.location_id,
            pd.concept_group, pd.concept_id, pd.answer_concept_id, pd.encounter_date))) AS fhir_id,
  @FHIR_ID(per.uuid) AS patient_fhir_id,
  COALESCE(pd.date_updated, pd.last_updated_date, '1970-01-01 00:00:00') AS changed_at,
  JSON_OBJECT(
    'resourceType', 'Condition',
    'id', CONCAT('cond-', MD5(CONCAT_WS('|', pd.mspp_code, pd.encounter_id, pd.location_id,
            pd.concept_group, pd.concept_id, pd.answer_concept_id, pd.encounter_date))),
    'meta', JSON_OBJECT('tag', JSON_ARRAY(JSON_OBJECT(
              'system', @VAR('mspp_site_system', 'http://sedish-haiti.org/fhir/mspp-site'), 'code', pd.mspp_code))),
    'clinicalStatus', JSON_OBJECT(
              'coding', JSON_ARRAY(JSON_OBJECT(
                'system', 'http://terminology.hl7.org/CodeSystem/condition-clinical',
                'code', 'active', 'display', 'Active')),
              'text', 'Active'),
    'verificationStatus', JSON_OBJECT(
              'coding', JSON_ARRAY(JSON_OBJECT(
                'system', 'http://terminology.hl7.org/CodeSystem/condition-ver-status',
                'code', 'confirmed', 'display', 'Confirmed')),
              'text', 'Confirmed'),
    'category', JSON_ARRAY(JSON_OBJECT('coding', JSON_ARRAY(JSON_OBJECT(
              'system', 'http://terminology.hl7.org/CodeSystem/condition-category',
              'code', 'encounter-diagnosis', 'display', 'Encounter Diagnosis')))),
    'code', JSON_OBJECT(
              'coding', JSON_ARRAY(JSON_OBJECT(
                'code', COALESCE(dc.uuid, RPAD(CAST(COALESCE(pd.answer_concept_id, pd.concept_id) AS CHAR), 36, 'A')),
                'display', cn.name)),
              'text', cn.name),
    'subject', JSON_OBJECT('reference', CONCAT('Patient/', @FHIR_ID(per.uuid)), 'type', 'Patient'),
    'recordedDate', REPLACE(CAST(pd.encounter_date AS CHAR), ' ', 'T')
  ) AS resource
FROM consolidated_db.patient_diagnosis pd
JOIN consolidated_db.person_openmrs per
  ON per.mspp_code = pd.mspp_code AND per.person_id = pd.patient_id
LEFT JOIN consolidated_db.concept dc
  ON dc.concept_id = COALESCE(pd.answer_concept_id, pd.concept_id)
-- one preferred name per concept (a concept can have a preferred name per locale, which would
-- otherwise fan the row out N times); prefer English, else any preferred name.
LEFT JOIN (
  SELECT concept_id, COALESCE(MAX(CASE WHEN locale = 'en' THEN name END), MAX(name)) AS name
  FROM consolidated_db.concept_name
  WHERE locale_preferred = 1 AND COALESCE(voided, 0) = 0
  GROUP BY concept_id
) cn ON cn.concept_id = COALESCE(pd.answer_concept_id, pd.concept_id)
WHERE COALESCE(pd.voided, 0) = 0
