# services/storage/csv_operations.py
"""
CSV file operations for both local filesystem and S3 storage.
Handles saving, loading, and listing CSV files for data persistence.

This module provides utility functions for reading and writing pandas DataFrames
as CSV files, supporting both local disk and S3 backends. It ensures consistent
encoding and directory management for robust data storage.
"""
import io
import pandas as pd
from pathlib import Path

from config import USE_S3
from .s3_client import get_s3_client, parse_s3_path


def save_csv(df, path):
    """
    Save a pandas DataFrame as a CSV file to local filesystem or S3.
    
    Serializes the DataFrame to CSV format and writes it to the specified path.
    For S3, uploads the CSV data as an object. For local, writes to disk.
    
    Args:
        df (pd.DataFrame): DataFrame to save
        path (str): File path for saving (local or S3)
    
    Raises:
        Exception: If S3 upload fails or local file write fails
    """
    if USE_S3:
        bucket, key = parse_s3_path(str(path))
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding="utf-8")
        get_s3_client().put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    else:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False, encoding="utf-8")


def load_csv(path):
    """
    Load a CSV file from local filesystem or S3 into a pandas DataFrame.
    
    Reads a CSV file from the specified path and loads it into a DataFrame.
    For S3, downloads the object and loads it from memory. For local, reads from disk.
    
    Args:
        path (str): File path to load from (local or S3)
    
    Returns:
        pd.DataFrame: Loaded DataFrame
    
    Raises:
        FileNotFoundError: If file doesn't exist
        Exception: If S3 download fails or local file read fails
    """
    if USE_S3:
        bucket, key = parse_s3_path(str(path))
        obj = get_s3_client().get_object(Bucket=bucket, Key=key)
        return pd.read_csv(io.BytesIO(obj["Body"].read()), encoding="utf-8")
    else:
        return pd.read_csv(path, encoding="utf-8")
