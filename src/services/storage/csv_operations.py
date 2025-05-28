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
from botocore.exceptions import ClientError


def save_csv(df, path):
    """
    Save DataFrame as CSV to local filesystem or S3.

    Args:
        df (pd.DataFrame): DataFrame to save
        path (str): File path for saving

    Raises:
        ValueError: If DataFrame is empty or path is invalid
        ClientError: If S3 upload fails
        IOError: If local file write fails
    """
    if df is None or df.empty:
        raise ValueError("DataFrame cannot be None or empty")
    
    if USE_S3:
        try:
            bucket, key = parse_s3_path(str(path))
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, encoding="utf-8")
            get_s3_client().put_object(
                Bucket=bucket, 
                Key=key, 
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
        except ClientError as e:
            raise ClientError(f"Failed to upload CSV to S3: {e}")
        except Exception as e:
            raise Exception(f"Unexpected error saving CSV to S3: {e}")
    else:
        try:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(path, index=False, encoding="utf-8")
        except IOError as e:
            raise IOError(f"Failed to save CSV locally: {e}")


def load_csv(path):
    """
    Load CSV file from local filesystem or S3.

    Args:
        path (str): File path to load from

    Returns:
        pd.DataFrame: Loaded DataFrame

    Raises:
        FileNotFoundError: If file doesn't exist
        ClientError: If S3 download fails
        pd.errors.EmptyDataError: If CSV is empty
    """
    if USE_S3:
        try:
            bucket, key = parse_s3_path(str(path))
            s3_client = get_s3_client()
            
            # Check if object exists
            try:
                s3_client.head_object(Bucket=bucket, Key=key)
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    raise FileNotFoundError(f"S3 object not found: {path}")
                raise
                
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(io.BytesIO(obj["Body"].read()), encoding="utf-8")
            
        except ClientError as e:
            raise ClientError(f"Failed to download CSV from S3: {e}")
        except pd.errors.EmptyDataError:
            raise pd.errors.EmptyDataError(f"CSV file is empty: {path}")
    else:
        if not Path(path).exists():
            raise FileNotFoundError(f"Local file not found: {path}")
        try:
            return pd.read_csv(path, encoding="utf-8")
        except pd.errors.EmptyDataError:
            raise pd.errors.EmptyDataError(f"CSV file is empty: {path}")

