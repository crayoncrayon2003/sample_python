# scripts/plugins/transformers/__init__.py

"""
Transformer Plugins Sub-package

This package contains all plugins responsible for the 'Transform' phase of
the ETL process. These plugins take a DataContainer as input and perform
significant structural or format transformations on the data.

The goal of a transformer is to convert the source data into the schema
and format required by the destination (sink) system.

Examples of transformation tasks include:
- Executing SQL queries on the data (using DuckDB).
- Applying templates to structure data (using Jinja2).
- Converting tabular data to NGSI or NGSI-LD format.
- Adding, removing, or renaming columns.
- Aggregating data.
- Processing specific data formats like GTFS or Shapefiles.
"""

# As with other plugin packages, dynamic loading via the PluginRegistry
# means we do not need to explicitly import the classes here.