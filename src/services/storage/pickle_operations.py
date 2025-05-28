# services/storage/pickle_operations.py
"""
Pickle file operations for both local filesystem and S3 storage.
Handles saving and loading Python objects using pickle serialization.

This module provides utility functions for serializing Python objects to pickle
files and deserializing them back, supporting both local and S3 backends. It
ensures compatibility with cloud and on-premise storage for checkpointing and
state persistence.
"""
import io
import pickle
from pathlib import Path

from config import USE_S3
from .s3_client import get_s3_client, parse_s3_path


def save_pickle(obj, path):
    """
    Save a Python object as a pickle file to local filesystem or S3.
    
    Serializes the given Python object and writes it to the specified path.
    For S3, uploads the pickle data as an object. For local, writes to disk.
    
    Args:
        obj: Python object to save
        path (str): File path for saving (local or S3)
    
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
    Load a Python object from a pickle file (local filesystem or S3).
    
    Reads and deserializes a pickle file from the specified path.
    For S3, downloads the object and loads it from memory. For local, reads from disk.
    
    Args:
        path (str): File path to load from (local or S3)
    
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
