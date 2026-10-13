-- Run once in the Databricks SQL editor, after 00_unity_catalog.sql.
-- Monthly files from the open piebro/deutsche-bahn-data archive (CC BY 4.0) land here.

CREATE VOLUME IF NOT EXISTS bahn.bronze.archive;
