MODEL (
  name fhir.patient,
  kind INCREMENTAL_BY_UNIQUE_KEY (unique_key fhir_id),
  cron '*/5 * * * *',
  allow_partials true,
  start '2026-01-01',
  grain (mspp_code, patient_id),
  columns (
    mspp_code VARCHAR(10),
    patient_id INT,
    fhir_id VARCHAR(36),
    changed_at DATETIME,
    resource JSON
  ),
  audits (
    assert_patient_has_identifier,
    not_null(columns := (mspp_code, patient_id, fhir_id))
  )
);

/*
  consolidated_db identity rows -> FHIR Patient — shaped to match the OpenMRS fhir2
  PatientTranslator so the SHR sees the same structure the EMRs produce:
    * identifier: element id (uuid), use, type.text (from ref.identifier_systems label), system, value
    * name/address: element id (uuid); address detail in the fhir.openmrs.org/ext/address extension
    * deceasedBoolean / deceasedDateTime from person.dead / death_date
    * birthDate is year-only when birthdate_estimated
  NOT yet matched (need source we don't capture): the contained Provenance (creator ->
  Practitioner) and the identifier #location extension (location_id -> Location).

  Incremental: merged by uuid; changed_at = latest consolidated-server write across the
  patient's demographic tables.
*/
WITH names AS (
  SELECT mspp_code, person_id,
         JSON_ARRAYAGG(JSON_OBJECT(
           'id', uuid,
           'use', CASE WHEN preferred = 1 THEN 'official' ELSE 'usual' END,
           'family', family_name,
           'given', JSON_ARRAY(given_name))) AS arr,
         MAX(COALESCE(date_updated, date_created)) AS chg
  FROM consolidated_db.person_name_openmrs
  WHERE COALESCE(voided, 0) = 0
  GROUP BY mspp_code, person_id
),
addresses AS (
  SELECT mspp_code, person_id,
         JSON_ARRAYAGG(JSON_OBJECT(
           'id', uuid,
           'extension', JSON_ARRAY(JSON_OBJECT(
             'url', 'http://fhir.openmrs.org/ext/address',
             'extension', JSON_ARRAY(
               JSON_OBJECT('url', 'http://fhir.openmrs.org/ext/address#address1', 'valueString', address1),
               JSON_OBJECT('url', 'http://fhir.openmrs.org/ext/address#address2', 'valueString', address2),
               JSON_OBJECT('url', 'http://fhir.openmrs.org/ext/address#address3', 'valueString', address3)))),
           'use', 'home',
           'city', city_village,
           'state', state_province,
           'country', country)) AS arr,
         MAX(COALESCE(date_updated, date_created)) AS chg
  FROM consolidated_db.person_address_openmrs
  WHERE COALESCE(voided, 0) = 0
  GROUP BY mspp_code, person_id
),
idents AS (
  SELECT pi.mspp_code, pi.patient_id,
         JSON_OBJECT(
           'id', pi.uuid,
           'use', CASE WHEN pi.preferred = 1 THEN 'official' ELSE 'usual' END,
           'type', JSON_OBJECT('text', s.label),
           'system', s.system,
           'value', pi.identifier) AS ident,
         COALESCE(pi.date_updated, pi.date_created) AS chg
  FROM consolidated_db.patient_identifier_openmrs pi
  LEFT JOIN ref.identifier_systems s ON s.identifier_type = pi.identifier_type
  WHERE COALESCE(pi.voided, 0) = 0
  UNION ALL
  SELECT m.mspp_code, m.patient_id,
         JSON_OBJECT(
           'use', 'official',
           'type', JSON_OBJECT('text', 'National FP ID'),
           'system', @VAR('national_id_system'),
           'value', m.national_id),
         COALESCE(m.updated_at, m.created_at)
  FROM consolidated_db.national_fingerprint_mapping m
  WHERE m.national_id IS NOT NULL AND m.statut IN ('UNIQUE', 'DOUBLON')
),
identifiers AS (
  SELECT mspp_code, patient_id, JSON_ARRAYAGG(ident) AS arr, MAX(chg) AS chg
  FROM idents GROUP BY mspp_code, patient_id
),
-- all fingerprint mapping changes, regardless of statut — so a status downgrade
-- (UNIQUE → A_REVOIR) advances changed_at and triggers a re-sync to OpenCR even
-- though the identifier is no longer emitted (idents filters UNIQUE/DOUBLON only).
fp_chg AS (
  SELECT mspp_code, patient_id,
         MAX(COALESCE(updated_at, created_at)) AS chg
  FROM consolidated_db.national_fingerprint_mapping
  GROUP BY mspp_code, patient_id
),
-- phone -> telecom, sourced like fhir2: the 'Telephone Number' person attribute.
-- Feeds OpenCR's phone match rule (decisionRules: telecom.where(system='phone').value).
phones AS (
  SELECT pa.mspp_code, pa.person_id, MIN(pa.value) AS phone
  FROM consolidated_db.person_attribute_openmrs pa
  JOIN consolidated_db.person_attribute_type pat
    ON pat.person_attribute_type_id = pa.person_attribute_type_id
   AND pat.mspp_code = pa.mspp_code
  WHERE COALESCE(pa.voided, 0) = 0
    AND pat.name = @VAR('phone_attribute_name', 'Telephone Number')
    AND pa.value IS NOT NULL AND pa.value <> ''
  GROUP BY pa.mspp_code, pa.person_id
)
SELECT
  pt.mspp_code,
  pt.patient_id,
  per.uuid AS fhir_id,
  GREATEST(
    COALESCE(per.date_updated, per.date_created, '1970-01-01 00:00:00'),
    COALESCE(pt.date_updated,  pt.date_created,  '1970-01-01 00:00:00'),
    COALESCE(nm.chg,   '1970-01-01 00:00:00'),
    COALESCE(ad.chg,   '1970-01-01 00:00:00'),
    COALESCE(ids.chg,  '1970-01-01 00:00:00'),
    COALESCE(fp.chg,   '1970-01-01 00:00:00')
  ) AS changed_at,
  JSON_MERGE_PATCH(
   JSON_MERGE_PATCH(
    JSON_OBJECT(
      'resourceType', 'Patient',
      'id', per.uuid,
      -- facility provenance: originating site (mspp_code).
      'meta', JSON_OBJECT('tag', JSON_ARRAY(JSON_OBJECT(
                'system', 'http://sedish-haiti.org/fhir/mspp-site', 'code', pt.mspp_code))),
      'active', CAST(IF(COALESCE(per.voided, 0) = 0, 'true', 'false') AS JSON),
      'gender', CASE
                  WHEN per.gender IN ('M', 'Male') THEN 'male'
                  WHEN per.gender IN ('F', 'Female') THEN 'female'
                  WHEN per.gender IN ('O', 'Other') THEN 'other' ELSE 'unknown' END,
      'birthDate', CASE WHEN COALESCE(per.birthdate_estimated, 0) = 1
                        THEN CAST(YEAR(per.birthdate) AS CHAR)
                        ELSE CAST(per.birthdate AS CHAR) END,
      'name', nm.arr,
      'address', ad.arr,
      'identifier', ids.arr
    ),
    CASE
      WHEN COALESCE(per.dead, 0) = 1 AND per.death_date IS NOT NULL
        THEN JSON_OBJECT('deceasedDateTime', REPLACE(CAST(per.death_date AS CHAR), ' ', 'T'))
      WHEN COALESCE(per.dead, 0) = 1
        THEN JSON_OBJECT('deceasedBoolean', CAST('true' AS JSON))
      ELSE JSON_OBJECT('deceasedBoolean', CAST('false' AS JSON))
    END
   ),
   -- telecom (phone) only when present; absent attribute => no telecom (no breakage).
   CASE WHEN ph.phone IS NOT NULL
        THEN JSON_OBJECT('telecom', JSON_ARRAY(JSON_OBJECT(
               'system', 'phone', 'value', ph.phone, 'use', 'mobile')))
        ELSE JSON_OBJECT() END
  ) AS resource
FROM consolidated_db.patient_openmrs pt
JOIN consolidated_db.person_openmrs per
  ON per.mspp_code = pt.mspp_code AND per.person_id = pt.patient_id
LEFT JOIN names nm ON nm.mspp_code = pt.mspp_code AND nm.person_id = pt.patient_id
LEFT JOIN addresses ad ON ad.mspp_code = pt.mspp_code AND ad.person_id = pt.patient_id
LEFT JOIN identifiers ids ON ids.mspp_code = pt.mspp_code AND ids.patient_id = pt.patient_id
LEFT JOIN fp_chg fp  ON fp.mspp_code  = pt.mspp_code AND fp.patient_id  = pt.patient_id
LEFT JOIN phones ph  ON ph.mspp_code  = pt.mspp_code AND ph.person_id   = pt.patient_id
WHERE COALESCE(pt.voided, 0) = 0
