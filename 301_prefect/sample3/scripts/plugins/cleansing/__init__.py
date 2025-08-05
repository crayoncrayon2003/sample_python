# scripts/plugins/cleansing/__init__.py

"""
Cleansing Plugins Sub-package

This package contains all plugins responsible for the 'Cleansing' and
'Preparation' phase of the ETL process. These plugins take a DataContainer
as input and perform operations to clean, prepare, or preprocess the data
before it undergoes major transformation.

Examples of cleansing tasks include:
- Decompressing archive files (e.g., .zip, .tar.gz).
- Converting character encodings.
- Handling null or missing values.
- Removing duplicate records.
- Detecting the format of files.
"""

# As with other plugin packages, we don't need to export symbols here
# because they are loaded dynamically via the PluginRegistry.