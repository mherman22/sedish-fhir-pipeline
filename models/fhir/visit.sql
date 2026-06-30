MODEL (
  name fhir.visit,
  kind INCREMENTAL_BY_UNIQUE_KEY (unique_key fhir_id),
  cron '*/5 * * * *',
  allow_partials true,
  start '2026-01-01',
  grain (mspp_code, visit_id),
  columns (
    mspp_code VARCHAR(10),
    visit_id INT,
    fhir_id VARCHAR(36),
    patient_fhir_id VARCHAR(36),
    changed_at DATETIME,
    resource JSON
  ),
  audits (not_null(columns := (mspp_code, visit_id, fhir_id)))
);

/*
  visit_openmrs -> FHIR Encounter. In the OpenMRS fhir2 model BOTH visits and encounters are
  FHIR Encounters: a visit is distinguished by the encounter-tag code 'visit' (vs 'encounter'),
  and a real encounter links UP to its visit through Encounter.partOf (set in encounter.sql).
  The visit is the richer container — it carries the full admit/discharge period (date_started
  -> date_stopped) and the place of care, which the per-encounter resource (start only) does not.
  status is derived from date_stopped (finished once stopped, else in-progress) and period.end is
  emitted only when the visit has actually stopped.

  LIMITED vs fhir2: the consolidated extract has no visit-type label table, so Encounter.type is
  omitted (the IG marks type 1..*, but we keep runtime fidelity and don't stamp meta.profile — see
  the IG-conformance note); participant is likewise unavailable (no provider table).
*/
SELECT
  v.mspp_code,
  v.visit_id,
  @FHIR_ID(v.uuid) AS fhir_id,
  @FHIR_ID(per.uuid) AS patient_fhir_id,
  COALESCE(v.date_updated, v.date_changed, v.date_created, '1970-01-01 00:00:00') AS changed_at,
  JSON_MERGE_PATCH(
    JSON_OBJECT(
      'resourceType', 'Encounter',
      'id', @FHIR_ID(v.uuid),
      'meta', JSON_OBJECT('tag', JSON_ARRAY(
                JSON_OBJECT('system', 'http://fhir.openmrs.org/ext/encounter-tag',
                            'code', 'visit', 'display', 'Visit'),
                JSON_OBJECT('system', @VAR('mspp_site_system', 'http://sedish-haiti.org/fhir/mspp-site'), 'code', v.mspp_code))),
      'status', CASE WHEN v.date_stopped IS NOT NULL THEN 'finished' ELSE 'in-progress' END,
      'class', JSON_OBJECT('system', 'http://terminology.hl7.org/CodeSystem/v3-ActCode', 'code', 'AMB'),
      'subject', JSON_OBJECT('reference', CONCAT('Patient/', @FHIR_ID(per.uuid)), 'type', 'Patient'),
      'period', JSON_MERGE_PATCH(
                  JSON_OBJECT('start', REPLACE(CAST(v.date_started AS CHAR),' ','T')),
                  CASE WHEN v.date_stopped IS NOT NULL
                       THEN JSON_OBJECT('end', REPLACE(CAST(v.date_stopped AS CHAR),' ','T'))
                       ELSE JSON_OBJECT() END)
    ),
    -- location: resolve visit.location_id -> locations.value_reference -> Location/<id> (fhir2
    -- maps the visit location the same way as the encounter location). Omitted when unmapped.
    CASE WHEN el.value_reference IS NOT NULL
         THEN JSON_OBJECT('location', JSON_ARRAY(JSON_OBJECT(
                'location', JSON_OBJECT(
                  'reference', CONCAT('Location/', @FHIR_ID(el.value_reference)),
                  'type', 'Location',
                  'display', el.name))))
         ELSE JSON_OBJECT() END
  ) AS resource
FROM consolidated_db.visit_openmrs v
JOIN consolidated_db.person_openmrs per
  ON per.mspp_code = v.mspp_code AND per.person_id = v.patient_id
-- locations is a GLOBAL reference table (no mspp_code) -> join on location_id only.
LEFT JOIN consolidated_db.locations el
  ON el.location_id = v.location_id
WHERE COALESCE(v.voided, 0) = 0
