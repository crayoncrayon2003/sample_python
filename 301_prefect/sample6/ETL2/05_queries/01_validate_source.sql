-- ETL2/05_queries/01_validate_source.sql
--
-- This query checks for potential issues in the source data.
-- It returns rows that fail any of the validation criteria.
-- If this query returns any rows, the data quality is questionable.

SELECT
    sensor_id,
    'Invalid temperature reading' AS validation_error,
    temperature
FROM
    measurements
WHERE
    -- Temperature should be within a plausible range (e.g., -50 to 100 Celsius)
    temperature < -50 OR temperature > 100

UNION ALL

SELECT
    sensor_id,
    'Invalid humidity reading' AS validation_error,
    humidity
FROM
    measurements
WHERE
    -- Humidity should be a percentage between 0 and 100
    humidity < 0 OR humidity > 100

UNION ALL

SELECT
    sensor_id,
    'Missing timestamp' AS validation_error,
    timestamp
FROM
    measurements
WHERE
    timestamp IS NULL OR timestamp = '';