# services/storage/file_management.py
"""
File management operations for both local filesystem and S3 storage.
Handles listing files and directory cleanup.
"""
from pathlib import Path

from config import USE_S3
from .s3_client import get_s3_client, parse_s3_path
from botocore.exceptions import ClientError

def list_files(directory, pattern="*"):
    """
    List files in a directory (local or S3) matching a pattern.

    Args:
        directory (str): Directory path to search in
        pattern (str): File pattern to match (e.g., "*.csv"). Defaults to "*".

    Returns:
        list: List of file paths matching the pattern

    Raises:
        ClientError: If S3 listing fails
        OSError: If local directory access fails
    """
    if USE_S3:
        try:
            bucket, prefix = parse_s3_path(str(directory))
            s3 = get_s3_client()
            paginator = s3.get_paginator("list_objects_v2")
            file_list = []

            # Convert glob pattern to simple endswith check for S3
            if pattern != "*":
                suffix = pattern.replace("*", "")
            else:
                suffix = ""

            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if not suffix or obj["Key"].endswith(suffix):
                        file_list.append(f"s3://{bucket}/{obj['Key']}")
            return file_list
            
        except ClientError as e:
            raise ClientError(f"Failed to list S3 objects: {e}")
    else:
        try:
            directory_path = Path(directory)
            if not directory_path.exists():
                return []
            return [str(p) for p in directory_path.glob(pattern)]
        except OSError as e:
            raise OSError(f"Failed to access local directory: {e}")


def delete_all(directory):
    """
    Delete all files in a directory (local or S3).

    Args:
        directory (str): Directory path to clean up

    Returns:
        dict: Summary of deletion operation
    """
    deletion_summary = {"deleted_count": 0, "errors": []}
    
    if USE_S3:
        try:
            bucket, prefix = parse_s3_path(str(directory))
            s3 = get_s3_client()
            objects_to_delete = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            delete_keys = [
                {"Key": obj["Key"]} for obj in objects_to_delete.get("Contents", [])
            ]
            if delete_keys:
                response = s3.delete_objects(
                    Bucket=bucket, 
                    Delete={"Objects": delete_keys}
                )
                deletion_summary["deleted_count"] = len(response.get("Deleted", []))
                if "Errors" in response:
                    deletion_summary["errors"] = response["Errors"]
        except ClientError as e:
            deletion_summary["errors"].append(f"S3 deletion failed: {e}")
    else:
        p = Path(directory)
        if not p.exists():
            return deletion_summary
            
        for f in p.rglob("*"):
            try:
                if f.is_file():
                    f.unlink()
                    deletion_summary["deleted_count"] += 1
                elif f.is_dir() and not any(f.iterdir()):  # Only delete empty dirs
                    f.rmdir()
            except Exception as e:
                deletion_summary["errors"].append(f"Failed to delete {f}: {e}")
    
    return deletion_summary