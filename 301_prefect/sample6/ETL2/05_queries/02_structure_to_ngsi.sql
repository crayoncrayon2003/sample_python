-- ETL2/05_queries/02_structure_to_ngsi.sql
--
-- This query transforms the wide-format measurements data into a structure
-- that is easy to use within the NGSI Jinja2 template.

SELECT
    -- Rename columns to match the variables used in the Jinja2 template
    sensor_id,
    "location" AS location_str,
    
    -- Split the location string into latitude and longitude
    CAST(SPLIT_PART("location", ',', 2) AS DOUBLE) AS longitude,
    CAST(SPLIT_PART("location", ',', 1) AS DOUBLE) AS latitude,

    -- Ensure temperature and humidity are numeric
    CAST(temperature AS DOUBLE) AS temp_value,
    CAST(humidity AS DOUBLE) AS humidity_value,

    -- Convert the timestamp string to a proper ISO 8601 format
    -- ★★★ The format specifier is updated to handle the trailing 'Z' ★★★
    STRFTIME(STRPTIME(timestamp, '%Y-%m-%dT%H:%M:%SZ'), '%Y-%m-%dT%H:%M:%SZ') AS observed_at_iso

FROM
    measurements
WHERE
    sensor_id IS NOT NULL
    AND temperature IS NOT NULL
    AND humidity IS NOT NULL
    AND "location" IS NOT NULL;