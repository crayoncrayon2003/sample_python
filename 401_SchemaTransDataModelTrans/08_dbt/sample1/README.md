# Create New Project
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
    ├── .gitignore
    ├── dbt_project.yml
    └── README.md


# Define project-dependent packages
./project/test_project/packages.yml

```
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.1
```

# Setting up Connection profiles
./project/test_project/profiles.yml

```
test_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ./duckdb/test.duckdb
```

# make db
```
mkdir -p ./project/test_project/duckdb
```

# build
```
cd ./project/test_project

# instal packages based on packages.yml
dbt deps

# confirm connection based on profiles.yml
dbt debug

# create model
dbt run
```

# commond lists
dbt run   : create model
dbt test  : run test
dbt deps  : get packages
dbt debug : confirm state
dbt docs generate && dbt docs serve : genarate docs
