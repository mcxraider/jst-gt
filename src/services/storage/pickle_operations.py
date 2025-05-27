# services/storage/pickle_operations.py
"""
Pickle file operations for both local filesystem and S3 storage.
Handles saving and loading Python objects using pickle.
"""
import io
import pickle
from pathlib import Path

from config import USE_S3
from .s3_client import get_s3_client, parse_s3_path


def save_pickle(obj, path):
    """
    Save Python object as pickle file to local filesystem or S3.

    Args:
        obj: Python object to save
        path (str): File path for saving

    Raises:
        Exception: If S3 upload fails or local file write fails
    """
    if USE_S3:
        bucket, key = parse_s3_path(str(path))
        buf = io.BytesIO()
        pickle.dump(obj, buf)
        buf.seek(0)
        get_s3_client().put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    else:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(obj, f)


def load_pickle(path):
    """
    Load Python object from pickle file (local filesystem or S3).

    Args:
        path (str): File path to load from

    Returns:
        object: Loaded Python object

    Raises:
        FileNotFoundError: If file doesn't exist
        Exception: If S3 download fails or local file read fails
    """
    if USE_S3:
        bucket, key = parse_s3_path(str(path))
        obj = get_s3_client().get_object(Bucket=bucket, Key=key)
        return pickle.load(io.BytesIO(obj["Body"].read()))
    else:
        with open(path, "rb") as f:
            return pickle.load(f)
