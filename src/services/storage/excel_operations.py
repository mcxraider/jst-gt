# services/storage/excel_operations.py
"""
Excel file operations for both local filesystem and S3 storage.
Handles saving and loading Excel files.
"""
import io
import pandas as pd
from pathlib import Path

from config import USE_S3
from .s3_client import get_s3_client, parse_s3_path


def save_excel(df, path):
    """
    Save DataFrame as Excel file to local filesystem or S3.

    Args:
        df (pd.DataFrame): DataFrame to save
        path (str): File path for saving

    Raises:
        Exception: If S3 upload fails or local file write fails
    """
    if USE_S3:
        print("no string: " + path + " with string: " + str(path))
        bucket, key = parse_s3_path(str(path))
        buffer = io.BytesIO()
        df.to_excel(buffer, index=False, engine="openpyxl")
        buffer.seek(0)
        get_s3_client().put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    else:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(path, index=False, engine="openpyxl")


def load_excel(path, usecols=None):
    """
    Load Excel file from local filesystem or S3.

    Args:
        path (str): File path to load from
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
