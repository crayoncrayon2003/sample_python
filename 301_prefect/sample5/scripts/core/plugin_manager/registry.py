# scripts/core/plugin_manager/registry.py

from typing import Dict, Optional

class PluginRegistry:
    """
    A central registry for all available plugins in the framework.

    This class holds a mapping from a simple plugin name (e.g., 'from_http')
    to the full import path of the plugin's class
    (e.g., 'scripts.plugins.extractors.from_http.HttpExtractor').

    The PluginManager uses this registry to look up plugins. To add a new
    plugin to the framework, it must be registered here.
    """
    def __init__(self):
        self._plugins: Dict[str, str] = {}

    def register_plugin(self, name: str, class_path: str):
        """
        Registers a new plugin or updates an existing one.

        Args:
            name (str): The short, unique name for the plugin used in config files.
            class_path (str): The full, importable path to the plugin class.
        """
        if name in self._plugins:
            # This can be useful for debugging if plugins are accidentally overridden.
            print(f"Warning: Re-registering plugin '{name}'.")
        self._plugins[name] = class_path

    def get_plugin_path(self, name: str) -> Optional[str]:
        """
        Retrieves the import path for a registered plugin.

        Args:
            name (str): The name of the plugin to look up.

        Returns:
            Optional[str]: The full class path if the plugin is found, otherwise None.
        """
        return self._plugins.get(name)

# Create a single, global instance of the registry that will be used
# throughout the application. This follows the Singleton pattern in practice.
plugin_registry = PluginRegistry()


# =======================================================================
#                   REGISTER ALL FRAMEWORK PLUGINS HERE
# This is the central location to make the framework aware of new plugins.
# To add your own plugin, simply call plugin_registry.register_plugin()
# with its name and full class path.
# =======================================================================

# --- Extractor Plugins (Data Acquisition) ---
plugin_registry.register_plugin('from_local_file', 'scripts.plugins.extractors.from_local_file.LocalFileExtractor')
plugin_registry.register_plugin('from_local_json', 'scripts.plugins.extractors.from_local_json.LocalJsonExtractor')
plugin_registry.register_plugin('from_http', 'scripts.plugins.extractors.from_http.HttpExtractor')
plugin_registry.register_plugin('from_ftp', 'scripts.plugins.extractors.from_ftp.FtpExtractor')
plugin_registry.register_plugin('from_scp', 'scripts.plugins.extractors.from_scp.ScpExtractor')
plugin_registry.register_plugin('from_database', 'scripts.plugins.extractors.from_database.DatabaseExtractor')

# --- Cleansing Plugins (Data Cleaning & Preparation) ---
plugin_registry.register_plugin('archive_extractor', 'scripts.plugins.cleansing.archive_extractor.ArchiveExtractor')
plugin_registry.register_plugin('encoding_converter', 'scripts.plugins.cleansing.encoding_converter.EncodingConverter')
plugin_registry.register_plugin('format_detector', 'scripts.plugins.cleansing.format_detector.FormatDetector')
plugin_registry.register_plugin('duplicate_remover', 'scripts.plugins.cleansing.duplicate_remover.DuplicateRemover')
plugin_registry.register_plugin('null_handler', 'scripts.plugins.cleansing.null_handler.NullHandler')

# --- Transformer Plugins (Data Transformation) ---
plugin_registry.register_plugin('with_duckdb', 'scripts.plugins.transformers.with_duckdb.DuckDBTransformer')
plugin_registry.register_plugin('with_jinja2', 'scripts.plugins.transformers.with_jinja2.Jinja2Transformer')
plugin_registry.register_plugin('to_ngsi', 'scripts.plugins.transformers.to_ngsi.ToNxsiTransformer')
plugin_registry.register_plugin('csv_processor', 'scripts.plugins.transformers.csv_processor.CsvProcessor')
plugin_registry.register_plugin('json_processor', 'scripts.plugins.transformers.json_processor.JsonProcessor')
plugin_registry.register_plugin('gtfs_processor', 'scripts.plugins.transformers.gtfs_processor.GtfsProcessor')
plugin_registry.register_plugin('shapefile_processor', 'scripts.plugins.transformers.shapefile_processor.ShapefileProcessor')
plugin_registry.register_plugin('dataframe_joiner', 'scripts.plugins.transformers.dataframe_joiner.DataFrameJoiner')

# --- Validator Plugins (Data Validation) ---
plugin_registry.register_plugin('json_schema', 'scripts.plugins.validators.json_schema.JsonSchemaValidator')
plugin_registry.register_plugin('data_quality', 'scripts.plugins.validators.data_quality.DataQualityValidator')
plugin_registry.register_plugin('ngsi_validator', 'scripts.plugins.validators.ngsi_validator.NgsiValidator')
plugin_registry.register_plugin('business_rules', 'scripts.plugins.validators.business_rules.BusinessRulesValidator')

# --- Loader Plugins (Data Loading/Sinking) ---
plugin_registry.register_plugin('to_local_file', 'scripts.plugins.loaders.to_local_file.LocalFileLoader')
plugin_registry.register_plugin('to_http', 'scripts.plugins.loaders.to_http.HttpLoader')
plugin_registry.register_plugin('to_ftp', 'scripts.plugins.loaders.to_ftp.FtpLoader')
plugin_registry.register_plugin('to_scp', 'scripts.plugins.loaders.to_scp.ScpLoader')
plugin_registry.register_plugin('to_context_broker', 'scripts.plugins.loaders.to_context_broker.ContextBrokerLoader')
plugin_registry.register_plugin('to_database', 'scripts.plugins.loaders.to_database.DatabaseLoader')