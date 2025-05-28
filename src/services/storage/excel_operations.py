# services/storage/excel_operations.py
"""
Excel file operations for both local filesystem and S3 storage.
Handles saving and loading Excel files for data persistence.

This module provides utility functions for reading and writing pandas DataFrames
as Excel files, supporting both local disk and S3 backends. It ensures consistent
formatting and directory management for robust data storage.
"""
import io
import pandas as pd
from pathlib import Path

from config import USE_S3
from .s3_client import get_s3_client, parse_s3_path


def save_excel(df, path):
    """
    Save a pandas DataFrame as an Excel file to local filesystem or S3.
    
    Serializes the DataFrame to Excel format and writes it to the specified path.
    For S3, uploads the Excel data as an object. For local, writes to disk.
    
    Args:
        df (pd.DataFrame): DataFrame to save
        path (str): File path for saving (local or S3)
    
    Raises:
        Exception: If S3 upload fails or local file write fails
    """
    if USE_S3:
        bucket, key = parse_s3_path(str(path))
        excel_buffer = io.BytesIO()
        df.to_excel(excel_buffer, index=False, engine="openpyxl")
        get_s3_client().put_object(Bucket=bucket, Key=key, Body=excel_buffer.getvalue())
    else:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(path, index=False, engine="openpyxl")


def load_excel(path, usecols=None):
    """
    Load an Excel file from local filesystem or S3 into a pandas DataFrame.
    
    Reads an Excel file from the specified path and loads it into a DataFrame.
    For S3, downloads the object and loads it from memory. For local, reads from disk.
    Optionally loads only specified columns.
    
    Args:
        path (str): File path to load from (local or S3)
        usecols (list, optional): Columns to load. Defaults to None (all columns).
    
    Returns:
        pd.DataFrame: Loaded DataFrame
    
    Raises:
        FileNotFoundError: If file doesn't exist
        Exception: If S3 download fails or local file read fails
    """
    if USE_S3:
        bucket, key = parse_s3_path(str(path))
        obj = get_s3_client().get_object(Bucket=bucket, Key=key)
        return pd.read_excel(io.BytesIO(obj["Body"].read()), usecols=usecols)
    else:
        return pd.read_excel(path, usecols=usecols)
