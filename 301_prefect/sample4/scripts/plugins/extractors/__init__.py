# scripts/plugins/extractors/__init__.py

"""
Extractor Plugins Sub-package

This package contains all plugins responsible for the 'Extract' phase of
the ETL process. Each module in this package should define a class that
inherits from `ExtractorInterface` and implements the logic for fetching
data from a specific type of source (e.g., a local file, an HTTP API,
an FTP server, etc.).
"""

# This file can remain empty, or you could choose to import key extractors
# to make them available at the package level, for example:
#
# from .from_http import HttpExtractor
# from .from_local_file import LocalFileExtractor
#
# __all__ = [
#     'HttpExtractor',
#     'LocalFileExtractor',
# ]
#
# However, since plugins are loaded dynamically by the PluginManager via their
# full path, this is not strictly necessary for the framework to function.
# Keeping it simple is often the best approach.