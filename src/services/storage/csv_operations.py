# services/storage/csv_operations.py
"""
CSV file operations for both local filesystem and S3 storage.
Handles saving, loading, and listing CSV files.
"""
import io
import pandas as pd
from pathlib import Path

from config import USE_S3
from .s3_client import get_s3_client, parse_s3_path


def save_csv(df, path):
    """
    Save DataFrame as CSV to local filesystem or S3.

    Args:
        df (pd.DataFrame): DataFrame to save
        path (str): File path for saving

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
    Load CSV file from local filesystem or S3.

    Args:
        path (str): File path to load from

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
