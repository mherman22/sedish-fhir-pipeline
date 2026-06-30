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

/*
  encounter_openmrs -> FHIR Encounter.

  Encounter.location follows the OpenMRS fhir2 EncounterLocationTranslator: a single location[]
  component whose location reference points at the Location resource, with the location name as
  display. We resolve encounter.location_id through the consolidated `locations` table to its
  value_reference (the same key the fhir.location model is built on), so the reference resolves to
  a Location we actually emit. The `locations` table is a GLOBAL reference (no mspp_code), so the
  join is on location_id only. Like fhir2, no physicalType/status/period is set on the component.

  Encounter.partOf links this encounter UP to its visit, following the OpenMRS fhir2 model where a
  Visit is itself a FHIR Encounter (see visit.sql): resolve encounter.visit_id to the visit's uuid
  and reference it as Encounter/<visit-id>. The visit is emitted by fhir.visit and lands in the
  same per-patient bundle, so the reference resolves. partOf is omitted for encounters with no visit.

  Both location and partOf are omitted (no breakage) when their source is absent/unmapped.
*/
SELECT
  e.mspp_code,
  e.encounter_id,
  @FHIR_ID(e.uuid) AS fhir_id,
  @FHIR_ID(per.uuid) AS patient_fhir_id,
  COALESCE(e.date_updated, e.date_created, '1970-01-01 00:00:00') AS changed_at,
  JSON_MERGE_PATCH(
   JSON_MERGE_PATCH(
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
    ),
    -- location: resolve encounter.location_id -> locations.value_reference -> Location/<id>.
    -- Omitted (no breakage) when the location is unknown or unmapped in the locations table.
    CASE WHEN el.value_reference IS NOT NULL
         THEN JSON_OBJECT('location', JSON_ARRAY(JSON_OBJECT(
                'location', JSON_OBJECT(
                  'reference', CONCAT('Location/', @FHIR_ID(el.value_reference)),
                  'type', 'Location',
                  'display', el.name))))
         ELSE JSON_OBJECT() END
   ),
   -- partOf: link this encounter up to its visit (Encounter/<visit-uuid>); omitted when no visit.
   CASE WHEN v.uuid IS NOT NULL
        THEN JSON_OBJECT('partOf', JSON_OBJECT(
               'reference', CONCAT('Encounter/', @FHIR_ID(v.uuid)),
               'type', 'Encounter'))
        ELSE JSON_OBJECT() END
  ) AS resource
FROM consolidated_db.encounter_openmrs e
JOIN consolidated_db.person_openmrs per
  ON per.mspp_code = e.mspp_code AND per.person_id = e.patient_id
LEFT JOIN consolidated_db.encounter_type et
  ON et.encounter_type_id = e.encounter_type AND et.mspp_code = e.mspp_code
-- locations is a GLOBAL reference table (no mspp_code) -> join on location_id only.
LEFT JOIN consolidated_db.locations el
  ON el.location_id = e.location_id
-- the visit this encounter belongs to (for partOf); same site, matched on visit_id.
LEFT JOIN consolidated_db.visit_openmrs v
  ON v.visit_id = e.visit_id AND v.mspp_code = e.mspp_code
WHERE COALESCE(e.voided, 0) = 0
