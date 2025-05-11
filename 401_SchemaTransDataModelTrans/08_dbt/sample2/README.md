# 1.dbt
## Create New Project
```
mkdir project
cd project
dbt init

> Enter a name for your project (letters, digits, underscore): test_project
> The profile test already exists in /home/user/.dbt/profiles.yml. Continue and overwrite it? [y/N]: y
> Which database would you like to use?
> [1] duckdb
Enter a number: 1
```

project
├── logs/
└── test_project/
    ├── analyses/
    │   └── .gitkeep
    ├── macros/
    │   └── .gitkeep
    ├── models/
    │   └── example/
    │       ├── my_first_dbt_model.sql
    │       ├── my_second_dbt_model.sql
    │       └── schema.yml
    ├── seeds/
    │   └── .gitkeep
    ├── snapshots/
    │   └── .gitkeep
    ├── tests/
    │   └── .gitkeep
    ├── .gitkeep
    ├── dbt_project.yml
    └── README.md

## remove example
```
rm -R models/example
```

## modify dbt_project.yml
delete following
```
    # Config indicated by + and applies to all files under models/example/
    example:
```

add following
```
seeds:
  test_project:
    csv_data:
      file: "seeds/csv_data.csv"
      quote: '"'
      delimiter: ","
      escape: '"'
```

## create sample data
seeds/csv_data.csv

```
cat <<EOF > seeds/csv_data.csv
id,name,timestamp,value
1,Temperature Sensor,2025-05-11T12:00:00Z,23.5
2,Humidity Sensor,2025-05-11T12:10:00Z,45.2
EOF
```

## create sql
models/csv_to_ngsi_model.sql

```
WITH source_data AS (
    SELECT * FROM {{ ref('csv_data') }}
),
transformed AS (
    SELECT
        id,
        name,
        timestamp,
        value,
        'urn:ngsi-ld:Entity:' || id AS urn_id
    FROM source_data
)

SELECT
    urn_id AS id,
    'Sensor' AS type,
    json_build_object(
        'type', 'Property',
        'value', name
    ) AS name,
    json_build_object(
        'type', 'Property',
        'value', timestamp
    ) AS timestamp,
    json_build_object(
        'type', 'Property',
        'value', value
    ) AS value
FROM transformed
```

## Define project-dependent packages
None

## Setting up Connection profiles
None

## build
```
cd ./project/test_project

# instal packages based on packages.yml
dbt deps

# load seed
dbt seed

# confirm connection based on profiles.yml
dbt debug

# create model
dbt run
```

## Note
If you get a build error,
```
dbt clean
```

```
rm ./project/test_project/dev.duckdb
```


# 2. How to use
pip install duckdb

./sample2.py
```
import duckdb
con = duckdb.connect('path/to/your.duckdb')
result = con.execute("SELECT * FROM main.csv_to_ngsi_model").fetchall()
```