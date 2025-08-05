-- ETL1/05_queries/add_timestamp.sql
--
-- This SQL query is executed by DuckDB on the in-memory data.
-- It selects all original columns and adds a new column with the
-- current timestamp.

SELECT
    *,                          -- Select all columns from the original data
    NOW() AS processing_timestamp -- Add a new column with the current timestamp
FROM
    sales_data;                 -- The table name registered in the config.yml