-- Transforms the validated data into the target NGSI-v2 structure.
-- This includes creating URNs, adding a 'type' column, and renaming fields.
SELECT
    -- Dynamically generate a URN-based ID compliant with NGSI-LD specs
    'urn:ngsi-ld:' || '{{ ngsi_entity_type }}' || ':' || device_id || '_' || "timestamp" AS id,

    -- Add a fixed 'type' column from a configuration parameter
    '{{ ngsi_entity_type }}' AS type,

    -- Rename 'device_id' to 'refDevice'
    device_id AS refDevice,

    -- Rename 'timestamp' to 'dateObserved' and cast its type
    "timestamp"::TIMESTAMP AS dateObserved,

    -- Rename 'value' to 'measurement' and cast its type
    "value"::DOUBLE AS measurement
FROM
    source