-- ETL3/04_queries/transform_device_logs.sql
SELECT
    'urn:ngsi-ld:' || '{{ ngsi_entity_type }}' || ':' || deviceId AS id,
    '{{ ngsi_entity_type }}' AS type,

    deviceId AS refDevice,
    try_cast(recordedAt AS TIMESTAMP) AS dateObserved,
    try_cast(reading AS DOUBLE) AS measurement
FROM
    source
WHERE
    try_cast(recordedAt AS TIMESTAMP) IS NOT NULL
    AND
    reading IS NOT NULL