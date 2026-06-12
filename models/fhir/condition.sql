MODEL (
  name fhir.condition,
  kind INCREMENTAL_BY_UNIQUE_KEY (unique_key fhir_id),
  cron '*/5 * * * *',
  allow_partials true,
  start '2026-01-01',
  grain (fhir_id),
  audits (not_null(columns := (mspp_code, fhir_id)))
);

/*
  patient_diagnosis (iSantePlus DERIVED) -> FHIR Condition. This table is pre-filtered
  to diagnoses but has NO uuid, so the resource id is a deterministic synthetic key over
  (mspp_code, patient_id, encounter, obs_group, diagnosis concept). code = answer_concept_id
  (the coded diagnosis) via concept_name; optional encounter ref via JSON_MERGE_PATCH.
  verificationStatus/severity are intentionally omitted (suspected_confirmed/primary_secondary
  are concept ids we can't resolve without the CIEL dictionary).
*/
SELECT
  pd.mspp_code,
  pd.patient_id,
  CONCAT('cond-', MD5(CONCAT_WS('|', pd.mspp_code, pd.encounter_id, pd.location_id,
            pd.concept_group, pd.concept_id, pd.answer_concept_id, pd.encounter_date))) AS fhir_id,
  per.uuid AS patient_fhir_id,
  COALESCE(pd.date_updated, pd.last_updated_date, '1970-01-01 00:00:00') AS changed_at,
  JSON_MERGE_PATCH(
    JSON_OBJECT(
      'resourceType', 'Condition',
      'id', CONCAT('cond-', MD5(CONCAT_WS('|', pd.mspp_code, pd.encounter_id, pd.location_id,
              pd.concept_group, pd.concept_id, pd.answer_concept_id, pd.encounter_date))),
      'meta', JSON_OBJECT('tag', JSON_ARRAY(JSON_OBJECT(
                'system', 'http://sedish-haiti.org/fhir/mspp-site', 'code', pd.mspp_code))),
      'clinicalStatus', JSON_OBJECT(
                'coding', JSON_ARRAY(JSON_OBJECT(
                  'system', 'http://terminology.hl7.org/CodeSystem/condition-clinical',
                  'code', 'active', 'display', 'Active')),
                'text', 'Active'),
      'category', JSON_ARRAY(JSON_OBJECT('coding', JSON_ARRAY(JSON_OBJECT(
                'system', 'http://terminology.hl7.org/CodeSystem/condition-category',
                'code', 'encounter-diagnosis', 'display', 'Encounter Diagnosis')))),
      'code', JSON_OBJECT(
                'coding', JSON_ARRAY(JSON_OBJECT(
                  'code', COALESCE(dc.uuid, RPAD(CAST(COALESCE(pd.answer_concept_id, pd.concept_id) AS CHAR), 36, 'A')),
                  'display', cn.name)),
                'text', cn.name),
      'subject', JSON_OBJECT('reference', CONCAT('Patient/', per.uuid), 'type', 'Patient'),
      'recordedDate', REPLACE(CAST(pd.encounter_date AS CHAR), ' ', 'T')
    ),
    CASE WHEN enc.uuid IS NOT NULL
         THEN JSON_OBJECT('encounter', JSON_OBJECT('reference', CONCAT('Encounter/', enc.uuid)))
         ELSE JSON_OBJECT() END
  ) AS resource
FROM consolidated_db.patient_diagnosis pd
JOIN consolidated_db.person_openmrs per
  ON per.mspp_code = pd.mspp_code AND per.person_id = pd.patient_id
LEFT JOIN consolidated_db.encounter_openmrs enc
  ON enc.mspp_code = pd.mspp_code AND enc.encounter_id = pd.encounter_id
LEFT JOIN consolidated_db.concept dc
  ON dc.concept_id = COALESCE(pd.answer_concept_id, pd.concept_id)
LEFT JOIN consolidated_db.concept_name cn
  ON cn.concept_id = COALESCE(pd.answer_concept_id, pd.concept_id)
     AND cn.locale_preferred = 1 AND COALESCE(cn.voided, 0) = 0
WHERE COALESCE(pd.voided, 0) = 0
