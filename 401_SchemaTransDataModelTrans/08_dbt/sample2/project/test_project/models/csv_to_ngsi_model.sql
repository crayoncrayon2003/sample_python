SELECT json_group_array(
    json_object(
        'id', id,
        'name', name,
        'timestamp', timestamp,
        'value', value
    )
) AS json_result
FROM {{ ref('csv_data') }}
