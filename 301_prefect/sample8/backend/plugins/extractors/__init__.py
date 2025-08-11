# backend/plugins/extractors/__init__.py

"""
Extractor Plugins Sub-package
"""

# Import all modules in this package so pluggy can find the hook implementations.
from . import from_local_file
from . import from_http
from . import from_ftp
from . import from_scp
from . import from_database