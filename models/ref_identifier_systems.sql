MODEL (
  name fhir.identifier_systems,
  kind SEED (path '../seeds/ref_identifier_systems.csv'),
  columns (identifier_type INT, system TEXT, label TEXT)
);
