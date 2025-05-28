# services/storage/__init__.py
"""
Storage service package for file operations across local and S3 backends.

This package provides unified interfaces for reading, writing, listing, and managing
CSV, Excel, and pickle files, as well as S3 client utilities. It abstracts away the
differences between local and cloud storage, enabling seamless data persistence
and retrieval for the application.
"""
from .csv_operations import save_csv, load_csv
from .excel_operations import save_excel, load_excel
from .pickle_operations import save_pickle, load_pickle
from .file_management import list_files, delete_all

__all__ = [
    "save_csv",
    "load_csv",
    "save_excel",
    "load_excel",
    "save_pickle",
    "load_pickle",
    "list_files",
    "delete_all",
]
