MODEL (
  name fhir.location,
  kind FULL,
  cron '*/5 * * * *',
  grain (fhir_id),
  columns (
    fhir_id VARCHAR(255),
    resource JSON
  ),
  audits (not_null(columns := (fhir_id)))
);

/*
  locations -> FHIR Location. GLOBAL reference resource: not patient-scoped, no mspp_code,
  no change timestamp — so kind FULL (small table) and the loader pushes it via its global
  path (not the per-patient bundle). address detail in the fhir2 ext/address extension.

  LIMITED vs fhir2: the consolidated `locations` table is a thin code/name reference, so id =
  value_reference (no OpenMRS location uuid -> won't reconcile with the EMR's uuid-keyed
  Locations), and partOf / country / contained Provenance / narrative are absent. Full
  fidelity needs the OpenMRS location table (uuid, parent, country, creator).
*/
SELECT
  @FHIR_ID(l.value_reference) AS fhir_id,
  JSON_OBJECT(
    'resourceType', 'Location',
    'id', @FHIR_ID(l.value_reference),
    'name', l.name,
    'status', CASE WHEN COALESCE(l.active, 1) = 1 THEN 'active' ELSE 'inactive' END,
    'address', JSON_OBJECT(
      'extension', JSON_ARRAY(JSON_OBJECT(
        'url', 'http://fhir.openmrs.org/ext/address',
        'extension', JSON_ARRAY(JSON_OBJECT(
          'url', 'http://fhir.openmrs.org/ext/address#address3', 'valueString', l.address3)))),
      'city', l.city_village,
      'state', l.state_province)
  ) AS resource
FROM consolidated_db.locations l
WHERE l.value_reference IS NOT NULL
