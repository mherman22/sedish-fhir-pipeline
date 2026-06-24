-- ============================================================================
-- Phase 1 pre-flight: verify the identifier_type -> FHIR system mapping
-- ============================================================================
-- Run against the consolidated_db (the ETL's source) BEFORE go-live.
--
-- WHY: the ETL stamps an identifier system per OpenMRS identifier_type via the
-- seed `seeds/ref_identifier_systems.csv` (type 3 -> iSantePlus ID, 4 -> Code ST,
-- 5 -> Code National, 6 -> Biometrics National Reference Code, 9 -> Code PC).
-- These IDs match the standard iSantePlus init dump. OpenCR's decisionRules.json
-- matches ONLY on the canonical systems (…/3-isanteplus-id, …/5-code-national,
-- and …/6-biometrics-national-reference-code). If the real consolidated_db uses
-- different IDs, we'd stamp the wrong (or NULL) system and OpenCR would silently
-- never match — so confirm the IDs below line up with the seed, and add any
-- matched-on type the seed is missing.
--
-- Expected: the rows for "iSantePlus ID" / "Code National" line up with seed
-- type_ids 3 and 5. Anything else with a high count is a candidate the seed may
-- need to cover.

SELECT
  pi.identifier_type                AS type_id,
  MAX(pit.name)                     AS type_name,
  COUNT(*)                          AS num_identifiers,
  COUNT(DISTINCT pi.mspp_code)      AS num_sites,
  MIN(pi.identifier)                AS example_value
FROM patient_identifier_openmrs pi
LEFT JOIN patient_identifier_type pit
  ON pit.patient_identifier_type_id = pi.identifier_type
 AND pit.mspp_code = pi.mspp_code
GROUP BY pi.identifier_type
ORDER BY num_identifiers DESC;

-- ----------------------------------------------------------------------------
-- Companion: confirm the person-attribute type that holds the phone number.
-- The Patient model reads telecom from the attribute named 'Telephone Number'
-- (override via the SQLMesh var `phone_attribute_name`). Confirm that name is
-- what this deployment actually uses, and that values exist.
-- ----------------------------------------------------------------------------
SELECT
  pat.name                          AS attribute_type,
  COUNT(*)                          AS num_values,
  COUNT(DISTINCT pa.mspp_code)      AS num_sites
FROM person_attribute_openmrs pa
JOIN person_attribute_type pat
  ON pat.person_attribute_type_id = pa.person_attribute_type_id
 AND pat.mspp_code = pa.mspp_code
WHERE COALESCE(pa.voided, 0) = 0
GROUP BY pat.name
ORDER BY num_values DESC;
