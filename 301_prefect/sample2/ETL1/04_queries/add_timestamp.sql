-- ETL1/04_queries/add_timestamp.sql
SELECT
    *,
    NOW() AS "{{ new_column_name }}"
FROM source