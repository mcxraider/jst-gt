# services/storage/__init__.py
"""
Storage services package.
Provides unified interface for file operations across local filesystem and S3.
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
