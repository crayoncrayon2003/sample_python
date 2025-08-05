-- Pivot the long-format data into a wide-format table.
-- This version aggregates the metrics to ensure temperature and humidity
-- appear on the same row for a given sensor and timestamp.

SELECT
    sensor_id,
    timestamp,
    location,
    -- Use an aggregate function like MAX to pick the non-null value
    -- for each group. Since there's only one value per group, this works.
    MAX(CASE WHEN metric = 'temperature' THEN value END) AS temperature,
    MAX(CASE WHEN metric = 'humidity' THEN value END) AS humidity
FROM
    measurements
GROUP BY
    -- Group by the identifiers for a single measurement event
    sensor_id,
    timestamp,
    location;