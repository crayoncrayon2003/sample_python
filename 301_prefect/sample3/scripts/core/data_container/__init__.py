# scripts/core/data_container/__init__.py

"""
Data Container Module

This module provides the data structures used to pass data between
pipeline steps. The central component is the `DataContainer` class,
which acts as a standardized wrapper for various data formats
and associated metadata.
"""

# Import the main DataContainer class to make it directly accessible
# from the 'data_container' package.
from .container import DataContainer

# Import the enumeration of supported data formats.
from .formats import SupportedFormats

# Define the public API for the 'data_container' package.
__all__ = [
    'DataContainer',
    'SupportedFormats',
]