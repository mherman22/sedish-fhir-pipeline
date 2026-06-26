MODEL (
  name fhir.observation,
  kind INCREMENTAL_BY_UNIQUE_KEY (unique_key fhir_id),
  cron '*/5 * * * *',
  allow_partials true,
  start '2026-01-01',
  grain (mspp_code, obs_id),
  columns (
    mspp_code VARCHAR(10),
    obs_id INT,
    fhir_id VARCHAR(36),
    patient_fhir_id VARCHAR(36),
    changed_at DATETIME,
    resource JSON
  ),
  audits (assert_observation_has_subject)
);

SELECT
  o.mspp_code,
  o.obs_id,
  @FHIR_ID(o.uuid) AS fhir_id,
  @FHIR_ID(per.uuid) AS patient_fhir_id,
  COALESCE(o.date_updated, o.date_created, '1970-01-01 00:00:00') AS changed_at,
  JSON_MERGE_PATCH(
    JSON_MERGE_PATCH(
      JSON_OBJECT(
        'resourceType', 'Observation',
        'id', @FHIR_ID(o.uuid),
        'meta', JSON_OBJECT('tag', JSON_ARRAY(JSON_OBJECT(
                  'system', @VAR('mspp_site_system', 'http://sedish-haiti.org/fhir/mspp-site'), 'code', o.mspp_code))),
        'status', 'final',
        -- category (US Core / IPS expect it; FHIR "Preferred" binding). Derived from the OpenMRS
        -- concept class_id (standard concept-class seed): Test/LabSet -> laboratory, Procedure ->
        -- procedure, Finding/Symptom -> exam, else survey. Verified on live data (class 1 = CD4/
        -- viral load = lab; class 5 = Weight/Height = finding). Refine to concept-class NAMES once
        -- the concept_class dimension is in the extract (it currently isn't).
        'category', JSON_ARRAY(JSON_OBJECT('coding', JSON_ARRAY(JSON_OBJECT(
                      'system', 'http://terminology.hl7.org/CodeSystem/observation-category',
                      'code', CASE qc.class_id
                                WHEN 1  THEN 'laboratory'
                                WHEN 8  THEN 'laboratory'
                                WHEN 2  THEN 'procedure'
                                WHEN 5  THEN 'exam'
                                WHEN 12 THEN 'exam'
                                WHEN 13 THEN 'exam'
                                ELSE 'survey' END)))),
        'code', JSON_OBJECT(
                  'coding', JSON_ARRAY(JSON_OBJECT(
                              'code', COALESCE(qc.uuid, RPAD(CAST(o.concept_id AS CHAR), 36, 'A')),
                              'display', cn.name)),
                  'text', cn.name),
        'subject', JSON_OBJECT('reference', CONCAT('Patient/', @FHIR_ID(per.uuid)), 'type', 'Patient'),
        'effectiveDateTime', REPLACE(CAST(o.obs_datetime AS CHAR),' ','T'),
        'issued', REPLACE(CAST(o.date_created AS CHAR),' ','T')
      ),
      CASE
        WHEN o.value_numeric  IS NOT NULL THEN JSON_OBJECT('valueQuantity',
                                               JSON_MERGE_PATCH(
                                                 JSON_OBJECT('value', o.value_numeric),
                                                 CASE WHEN o.value_modifier IS NOT NULL
                                                      THEN JSON_OBJECT('comparator', o.value_modifier)
                                                      ELSE JSON_OBJECT() END))
        WHEN o.value_coded    IS NOT NULL THEN JSON_OBJECT('valueCodeableConcept',
                                               JSON_OBJECT('coding', JSON_ARRAY(JSON_OBJECT(
                                                 'code', COALESCE(vc.uuid, RPAD(CAST(o.value_coded AS CHAR), 36, 'A')),
                                                 'display', vcn.name))))
        WHEN o.value_datetime IS NOT NULL THEN JSON_OBJECT('valueDateTime', REPLACE(CAST(o.value_datetime AS CHAR),' ','T'))
        WHEN o.value_text     IS NOT NULL THEN JSON_OBJECT('valueString', o.value_text)
        WHEN o.value_drug     IS NOT NULL THEN JSON_OBJECT('valueCodeableConcept',
                                               JSON_OBJECT('coding', JSON_ARRAY(JSON_OBJECT(
                                                 'system', @VAR('drug_system', 'http://isanteplus.org/openmrs/drug'),
                                                 'code',   CAST(o.value_drug AS CHAR)))))
        ELSE JSON_OBJECT()
      END
    ),
    CASE WHEN enc.uuid IS NOT NULL
         THEN JSON_OBJECT('encounter', JSON_OBJECT('reference', CONCAT('Encounter/', @FHIR_ID(enc.uuid))))
         ELSE JSON_OBJECT() END
  ) AS resource
FROM consolidated_db.obs_openmrs o
JOIN consolidated_db.person_openmrs per
  ON per.mspp_code = o.mspp_code AND per.person_id = o.person_id
LEFT JOIN consolidated_db.encounter_openmrs enc
  ON enc.mspp_code = o.mspp_code AND enc.encounter_id = o.encounter_id
LEFT JOIN consolidated_db.concept qc ON qc.concept_id = o.concept_id
LEFT JOIN consolidated_db.concept vc ON vc.concept_id = o.value_coded
-- preferred name for the obs question concept (code.coding.display)
LEFT JOIN (
  SELECT concept_id, COALESCE(MAX(CASE WHEN locale = 'en' THEN name END), MAX(name)) AS name
  FROM consolidated_db.concept_name
  WHERE locale_preferred = 1 AND COALESCE(voided, 0) = 0
  GROUP BY concept_id
) cn ON cn.concept_id = o.concept_id
-- preferred name for the coded answer concept (valueCodeableConcept.coding.display)
LEFT JOIN (
  SELECT concept_id, COALESCE(MAX(CASE WHEN locale = 'en' THEN name END), MAX(name)) AS name
  FROM consolidated_db.concept_name
  WHERE locale_preferred = 1 AND COALESCE(voided, 0) = 0
  GROUP BY concept_id
) vcn ON vcn.concept_id = o.value_coded
WHERE COALESCE(o.voided, 0) = 0
