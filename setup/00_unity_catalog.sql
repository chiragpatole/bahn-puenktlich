-- Run once in the Databricks SQL editor.
-- If CREATE CATALOG is refused in your workspace, use the default catalog
-- instead and change DATABRICKS_VOLUME_PATH to match.

CREATE CATALOG IF NOT EXISTS bahn;

CREATE SCHEMA IF NOT EXISTS bahn.bronze;
CREATE SCHEMA IF NOT EXISTS bahn.silver;
CREATE SCHEMA IF NOT EXISTS bahn.gold;

-- Raw files from the collector land here: /Volumes/bahn/bronze/landing
CREATE VOLUME IF NOT EXISTS bahn.bronze.landing;
