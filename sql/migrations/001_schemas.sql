-- 001 The five schemas. Data only moves forward: raw, clean, ontology, analysis.
-- control already exists: migrate creates it, with control.schema_migration before
-- it applies any file.
CREATE SCHEMA raw;
CREATE SCHEMA clean;
CREATE SCHEMA ontology;
CREATE SCHEMA analysis;
CREATE SCHEMA IF NOT EXISTS control;