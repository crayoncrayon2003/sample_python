# scripts/plugins/validators/__init__.py

"""
Validator Plugins Sub-package

This package contains all plugins responsible for the 'Validation' phase of
the ETL process. These plugins inspect the data within a DataContainer
to ensure it conforms to predefined rules, schemas, or quality standards.

A validator typically does not transform the data itself. Instead, it either
passes the data through unchanged if validation succeeds, or raises an
exception if validation fails, thus halting the pipeline.

Examples of validation tasks include:
- Validating JSON data against a JSON Schema.
- Performing data quality checks (e.g., checking for nulls, range checks).
- Validating NGSI entities against the NGSI-LD specification.
- Enforcing custom business rules (e.g., ensuring a value in one column
  is always greater than another).
"""

# Dynamic loading via the PluginRegistry means we do not need to
# explicitly import the classes here.