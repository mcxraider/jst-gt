# services/storage/file_management.py
"""
File management operations for both local filesystem and S3 storage.
Handles listing, pattern matching, and cleanup of files and directories.

This module provides utility functions for listing files matching a pattern
and deleting all files in a directory, supporting both local and S3 backends.
Pattern matching for S3 is simplified to suffix checks.
"""
from pathlib import Path

from config import USE_S3
from .s3_client import get_s3_client, parse_s3_path


def list_files(directory, pattern="*"):
    """
    List files in a directory (local or S3) matching a pattern.
    
    For local directories, uses glob pattern matching. For S3, matches files
    whose keys end with the specified suffix (pattern must be of the form '*.ext').
    
    Args:
        directory (str): Directory path to search in
        pattern (str): File pattern to match (e.g., '*.csv'). Defaults to '*'.
    
    Returns:
        list: List of file paths (local) or S3 keys (S3) matching the pattern
    
    Note:
        For S3, pattern matching is simplified to endswith() check.
    """
    if USE_S3:
        bucket, prefix = parse_s3_path(str(directory))
        s3 = get_s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        file_list = []

        # Convert glob pattern to simple endswith check for S3
        suffix = pattern.replace("*", "")

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(suffix):
                    file_list.append(obj["Key"])
        return file_list
    else:
        return list(Path(directory).glob(pattern))


def delete_all(directory):
    """
    Delete all files in a directory (local or S3).
    
    For S3, deletes all objects under the given prefix. For local directories,
    recursively deletes all files and attempts to remove empty directories.
    Silently ignores errors during deletion.
    
    Args:
        directory (str): Directory path to clean up
    
    Returns:
        None
    
    Note:
        For local filesystem, attempts to remove files and empty directories.
        Silently ignores errors during deletion process.
    """
    if USE_S3:
        bucket, prefix = parse_s3_path(str(directory))
        s3 = get_s3_client()
        objects_to_delete = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        delete_keys = [
            {"Key": obj["Key"]} for obj in objects_to_delete.get("Contents", [])
        ]
        if delete_keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": delete_keys})
    else:
        p = Path(directory)
        if not p.exists():
            return
        for f in p.rglob("*"):
            try:
                if f.is_file():
                    f.unlink()
                elif f.is_dir():
                    f.rmdir()
            except Exception:
                pass  # Silently ignore deletion errors
