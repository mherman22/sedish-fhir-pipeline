MODEL (
  name fhir.allergy_intolerance,
  kind INCREMENTAL_BY_UNIQUE_KEY (unique_key fhir_id),
  cron '*/5 * * * *',
  allow_partials true,
  start '2026-01-01',
  grain (mspp_code, allergy_id),
  columns (
    mspp_code VARCHAR(10),
    allergy_id INT,
    fhir_id VARCHAR(36),
    patient_fhir_id VARCHAR(36),
    changed_at DATETIME,
    resource JSON
  ),
  audits (not_null(columns := (mspp_code, allergy_id, fhir_id)))
);

/*
  allergy_openmrs -> FHIR AllergyIntolerance, shaped to match the OpenMRS fhir2
  AllergyIntoleranceTranslator. Concept coding follows fhir2: the primary coding is the
  concept UUID with NO system (we COALESCE the real concept.uuid, else derive the OpenMRS
  legacy/CIEL UUID = concept_id right-padded with 'A' to 36 chars, e.g. 5089 -> 5089AAA…).
  A non-coded allergen becomes code.text only. clinicalStatus/verificationStatus carry text;
  references carry type. (Still gated on source: the CIEL secondary coding from
  concept_reference_term, recorder→Practitioner, and reaction.severity/substance concept maps.)
*/
WITH reactions AS (
  SELECT ar.mspp_code, ar.allergy_id,
         JSON_ARRAYAGG(JSON_OBJECT(
           'manifestation', JSON_ARRAY(JSON_OBJECT(
             'coding', JSON_ARRAY(JSON_OBJECT(
               'code', COALESCE(rc.uuid, RPAD(CAST(ar.reaction_concept_id AS CHAR), 36, 'A')))),
             'text', ar.reaction_non_coded)))) AS arr,
         MAX(COALESCE(ar.date_updated, '1970-01-01 00:00:00')) AS chg
  FROM consolidated_db.allergy_reaction_openmrs ar
  LEFT JOIN consolidated_db.concept rc ON rc.concept_id = ar.reaction_concept_id
  GROUP BY ar.mspp_code, ar.allergy_id
)
SELECT
  a.mspp_code,
  a.allergy_id,
  @FHIR_ID(a.uuid) AS fhir_id,
  @FHIR_ID(per.uuid) AS patient_fhir_id,
  GREATEST(
    COALESCE(a.date_updated, a.date_created, '1970-01-01 00:00:00'),
    COALESCE(r.chg, '1970-01-01 00:00:00')
  ) AS changed_at,
  JSON_MERGE_PATCH(
    JSON_OBJECT(
      'resourceType', 'AllergyIntolerance',
      'id', @FHIR_ID(a.uuid),
      'meta', JSON_OBJECT('tag', JSON_ARRAY(JSON_OBJECT(
                'system', @VAR('mspp_site_system', 'http://sedish-haiti.org/fhir/mspp-site'), 'code', a.mspp_code))),
      'type', 'allergy',
      'clinicalStatus', JSON_OBJECT(
                'coding', JSON_ARRAY(JSON_OBJECT(
                  'system', 'http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical',
                  'code', 'active', 'display', 'Active')),
                'text', 'Active'),
      'verificationStatus', JSON_OBJECT(
                'coding', JSON_ARRAY(JSON_OBJECT(
                  'system', 'http://terminology.hl7.org/CodeSystem/allergyintolerance-verification',
                  'code', 'confirmed', 'display', 'Confirmed')),
                'text', 'Confirmed'),
      'category', JSON_ARRAY(CASE a.allergen_type
                               WHEN 'DRUG' THEN 'medication'
                               WHEN 'FOOD' THEN 'food'
                               WHEN 'ENVIRONMENT' THEN 'environment'
                               ELSE 'medication' END),
      'patient', JSON_OBJECT('reference', CONCAT('Patient/', @FHIR_ID(per.uuid)), 'type', 'Patient'),
      'recordedDate', REPLACE(CAST(a.date_created AS CHAR), ' ', 'T'),
      'code', CASE
                WHEN a.coded_allergen IS NOT NULL THEN JSON_OBJECT(
                  'coding', JSON_ARRAY(JSON_OBJECT(
                    'code', COALESCE(c.uuid, RPAD(CAST(a.coded_allergen AS CHAR), 36, 'A')),
                    'display', cn.name)),
                  'text', COALESCE(cn.name, a.non_coded_allergen))
                ELSE JSON_OBJECT('text', a.non_coded_allergen) END
    ),
    JSON_MERGE_PATCH(
      CASE WHEN r.arr IS NOT NULL THEN JSON_OBJECT('reaction', r.arr) ELSE JSON_OBJECT() END,
      CASE WHEN a.comment IS NOT NULL
           THEN JSON_OBJECT('note', JSON_ARRAY(JSON_OBJECT('text', a.comment))) ELSE JSON_OBJECT() END
    )
  ) AS resource
FROM consolidated_db.allergy_openmrs a
JOIN consolidated_db.person_openmrs per
  ON per.mspp_code = a.mspp_code AND per.person_id = a.patient_id
LEFT JOIN consolidated_db.concept c ON c.concept_id = a.coded_allergen
-- one preferred name per concept (a concept can have a preferred name per locale, which would
-- otherwise fan the row out N times); prefer English, else any preferred name.
LEFT JOIN (
  SELECT concept_id, COALESCE(MAX(CASE WHEN locale = 'en' THEN name END), MAX(name)) AS name
  FROM consolidated_db.concept_name
  WHERE locale_preferred = 1 AND COALESCE(voided, 0) = 0
  GROUP BY concept_id
) cn ON cn.concept_id = a.coded_allergen
LEFT JOIN reactions r
  ON r.mspp_code = a.mspp_code AND r.allergy_id = a.allergy_id
WHERE COALESCE(a.voided, 0) = 0
