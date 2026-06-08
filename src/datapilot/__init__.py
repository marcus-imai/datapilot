"""Datapilot — lightweight data processing pipeline framework."""

__version__ = "0.3.1"
__author__ = "Marcus Imai"
__license__ = "MIT"

from datapilot.pipeline import Pipeline
from datapilot.connectors import CSVSource, JSONSource, ParquetSource

__all__ = ["Pipeline", "CSVSource", "JSONSource", "ParquetSource", "__version__"]