# scripts/plugins/loaders/__init__.py

"""
Loader Plugins Sub-package

This package contains all plugins responsible for the 'Load' phase of
the ETL process. These plugins take the final, processed DataContainer
and load its contents into a target system, also known as a data sink.

A loader plugin is typically the final step in a pipeline and often
does not return a DataContainer, as there are no further steps.

Examples of loading tasks include:
- Writing data to a local file (e.g., CSV, Parquet, JSON).
- Uploading data to an HTTP endpoint (e.g., via POST or PUT requests).
- Uploading files to an FTP or SCP server.
- Registering NGSI entities with a Context Broker.
- Inserting or updating records in a database.
"""

# Dynamic loading via the PluginRegistry means we do not need to
# explicitly import the classes here.