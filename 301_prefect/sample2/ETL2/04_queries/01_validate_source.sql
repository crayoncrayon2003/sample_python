-- Filters the source data, removing rows with invalid formats or types.
-- It ensures that downstream steps only receive clean, reliable data.
SELECT
    *
FROM
    source
WHERE
    -- Ensure 'timestamp' can be cast to a valid TIMESTAMP
    try_cast("timestamp" AS TIMESTAMP) IS NOT NULL
    AND
    -- Ensure 'value' can be cast to a valid DOUBLE
    try_cast("value" AS DOUBLE) IS NOT NULL