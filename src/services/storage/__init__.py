# services/storage/__init__.py
"""
Storage services package.
Provides unified interface for file operations across local filesystem and S3.
"""

from .parquet_operations import save_parquet, load_parquet, get_parquet_file_info
from .pickle_operations import save_pickle, load_pickle
from .file_management import list_files, delete_all

__all__ = [
    "save_parquet",
    "load_parquet",
    "get_parquet_file_info",
    "save_pickle",
    "load_pickle",
    "list_files",
    "delete_all",
]
