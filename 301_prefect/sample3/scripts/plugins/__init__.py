# scripts/plugins/__init__.py

"""
ETL Framework Plugins Package

This package contains all the concrete implementations of the plugin
interfaces defined in `scripts.core.plugin_manager.interfaces`.

Each sub-package corresponds to a specific stage of the ETL process:
- `extractors`: For data acquisition from various sources.
- `cleansing`: For data cleaning and preparation tasks.
- `transformers`: For transforming data structures and formats.
- `validators`: For enforcing data quality and schema rules.
- `loaders`: For loading data into target systems (sinks).

To add a new plugin, create a new module within the appropriate
sub-package and ensure it is registered in the PluginRegistry.
"""