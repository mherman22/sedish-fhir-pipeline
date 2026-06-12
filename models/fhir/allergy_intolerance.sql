MODEL (
  name fhir.allergy_intolerance,
  kind INCREMENTAL_BY_UNIQUE_KEY (unique_key fhir_id),
  cron '*/5 * * * *',
  allow_partials true,
  start '2026-01-01',
  grain (mspp_code, allergy_id),
  audits (not_null(columns := (mspp_code, allergy_id, fhir_id)))
);

/*
  allergy_openmrs -> FHIR AllergyIntolerance (+ reactions from allergy_reaction_openmrs).
  Raw OpenMRS allergy table: has uuid (resource id) + patient_id (subject). category from
  allergen_type; code from coded_allergen (concept_name) else non_coded_allergen text.
  Merged by uuid; changed_at = latest consolidated-server write; patient_fhir_id for the
  loader bundle; meta.tag carries the originating site (mspp_code).
*/
WITH reactions AS (
  SELECT mspp_code, allergy_id,
         JSON_ARRAYAGG(JSON_OBJECT(
           'manifestation', JSON_ARRAY(JSON_OBJECT(
             'coding', JSON_ARRAY(JSON_OBJECT(
               'system', 'http://isanteplus.org/openmrs/concept',
               'code', CAST(reaction_concept_id AS CHAR))),
             'text', reaction_non_coded)))) AS arr,
         MAX(COALESCE(date_updated, '1970-01-01 00:00:00')) AS chg
  FROM consolidated_db.allergy_reaction_openmrs
  GROUP BY mspp_code, allergy_id
)
SELECT
  a.mspp_code,
  a.allergy_id,
  a.uuid AS fhir_id,
  per.uuid AS patient_fhir_id,
  GREATEST(
    COALESCE(a.date_updated, a.date_created, '1970-01-01 00:00:00'),
    COALESCE(r.chg, '1970-01-01 00:00:00')
  ) AS changed_at,
  JSON_MERGE_PATCH(
    JSON_OBJECT(
      'resourceType', 'AllergyIntolerance',
      'id', a.uuid,
      'meta', JSON_OBJECT('tag', JSON_ARRAY(JSON_OBJECT(
                'system', 'http://sedish-haiti.org/fhir/mspp-site', 'code', a.mspp_code))),
      'clinicalStatus', JSON_OBJECT('coding', JSON_ARRAY(JSON_OBJECT(
                'system', 'http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical',
                'code', 'active'))),
      'category', JSON_ARRAY(CASE a.allergen_type
                               WHEN 'DRUG' THEN 'medication'
                               WHEN 'FOOD' THEN 'food'
                               WHEN 'ENVIRONMENT' THEN 'environment'
                               ELSE 'medication' END),
      'patient', JSON_OBJECT('reference', CONCAT('Patient/', per.uuid)),
      'code', JSON_OBJECT(
                'coding', JSON_ARRAY(JSON_OBJECT(
                  'system', 'http://isanteplus.org/openmrs/concept',
                  'code', CAST(a.coded_allergen AS CHAR),
                  'display', cn.name)),
                'text', COALESCE(cn.name, a.non_coded_allergen))
    ),
    CASE WHEN r.arr IS NOT NULL THEN JSON_OBJECT('reaction', r.arr) ELSE JSON_OBJECT() END
  ) AS resource
FROM consolidated_db.allergy_openmrs a
JOIN consolidated_db.person_openmrs per
  ON per.mspp_code = a.mspp_code AND per.person_id = a.patient_id
LEFT JOIN consolidated_db.concept_name cn
  ON cn.concept_id = a.coded_allergen AND cn.locale_preferred = 1 AND COALESCE(cn.voided, 0) = 0
LEFT JOIN reactions r
  ON r.mspp_code = a.mspp_code AND r.allergy_id = a.allergy_id
WHERE COALESCE(a.voided, 0) = 0
