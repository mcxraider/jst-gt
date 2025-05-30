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


def s3_list_files_by_filename_contains(directory, contains_string, file_ext=".csv"):
    """
    List S3 files in a directory whose filenames contain a substring (and optional extension).

    Args:
        directory (str): S3 prefix directory (e.g., 's3://bucket/prefix/')
        contains_string (str): Substring the filename must contain
        file_ext (str): File extension (e.g., '.csv'). Use "" for any.

    Returns:
        list: List of file paths matching the criteria

    Raises:
        ClientError: If S3 listing fails
        Exception: For any other error
    """
    try:
        bucket, prefix = parse_s3_path(str(directory))
        s3 = get_s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        matches = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                filename = key.split("/")[-1]
                if contains_string in filename and filename.endswith(file_ext):
                    matches.append(f"s3://{bucket}/{key}")
        return matches
    except ClientError as e:
        raise ClientError(f"Failed to list S3 objects in {directory}: {e}")
    except Exception as e:
        raise Exception(f"Unexpected error listing S3 files in {directory}: {e}")


import os
import shutil
from botocore.exceptions import ClientError

def delete_all(directory):
    """
    Delete all files in a given S3 prefix or local directory, but preserve the folder marker (if any) for S3.

    Args:
        directory (str): S3 directory/prefix path (e.g., 's3://bucket/prefix/')
                         or local directory path

    Returns:
        dict: Summary of deletion operation
    """
    deletion_summary = {"deleted_count": 0, "errors": []}

    if USE_S3:
        try:
            print(f"\n[DEBUG] Starting deletion in: {directory}")
            bucket, prefix = parse_s3_path(str(directory))
            # Normalize prefix: remove leading slash, ensure trailing slash
            prefix = prefix.lstrip("/")
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            print(f"[DEBUG] Bucket: {bucket}")
            print(f"[DEBUG] Prefix: {prefix}")
            s3 = get_s3_client()
            paginator = s3.get_paginator("list_objects_v2")
            delete_batch = []
            total_deleted = 0

            for page_num, page in enumerate(
                paginator.paginate(Bucket=bucket, Prefix=prefix)
            ):
                print(f"\n[DEBUG] Processing page {page_num + 1}")
                objects = page.get("Contents", [])
                print(f"[DEBUG] Number of objects in this page: {len(objects)}")
                if not objects:
                    continue
                for obj in objects:
                    key = obj["Key"]
                    print(f"[DEBUG] Found key: {key}")
                    # Skip the folder marker (which is the prefix itself)
                    if key == prefix.rstrip("/"):
                        print(f"[DEBUG] Skipping folder marker: {key}")
                        continue
                    # Also skip "pseudo-folder" markers for subfolders (e.g. key endswith "/" and has nothing after the slash)
                    if key.endswith("/") and key.count("/") == prefix.count("/"):
                        print(f"[DEBUG] Skipping pseudo-folder marker: {key}")
                        continue
                    print(f"[DEBUG] Adding key to delete_batch: {key}")
                    delete_batch.append({"Key": key})
                    if len(delete_batch) == 1000:
                        print(f"[DEBUG] Deleting batch of 1000 keys:")
                        for k in delete_batch:
                            print(f"  - {k['Key']}")
                        resp = s3.delete_objects(
                            Bucket=bucket, Delete={"Objects": delete_batch}
                        )
                        batch_deleted = len(resp.get("Deleted", []))
                        print(f"[DEBUG] Deleted {batch_deleted} objects in this batch")
                        total_deleted += batch_deleted
                        if "Errors" in resp:
                            print(f"[DEBUG] Errors in batch: {resp['Errors']}")
                            deletion_summary["errors"].extend(resp["Errors"])
                        delete_batch = []
                # Delete any remaining keys
                if delete_batch:
                    print(
                        f"[DEBUG] Deleting final batch of {len(delete_batch)} keys in this page:"
                    )
                    for k in delete_batch:
                        print(f"  - {k['Key']}")
                    resp = s3.delete_objects(
                        Bucket=bucket, Delete={"Objects": delete_batch}
                    )
                    batch_deleted = len(resp.get("Deleted", []))
                    print(f"[DEBUG] Deleted {batch_deleted} objects in this batch")
                    total_deleted += batch_deleted
                    if "Errors" in resp:
                        print(f"[DEBUG] Errors in final batch: {resp['Errors']}")
                        deletion_summary["errors"].extend(resp["Errors"])
                    delete_batch = []

            print(f"\n[DEBUG] Total deleted: {total_deleted}")
            deletion_summary["deleted_count"] = total_deleted
        except ClientError as e:
            print(f"[DEBUG] S3 deletion failed: {e}")
            deletion_summary["errors"].append(f"S3 deletion failed: {e}")

    else:
        # Local deletion
        try:
            print(f"[DEBUG] Deleting all files in local directory: {directory}")
            deleted_count = 0
            if os.path.exists(directory):
                for root, dirs, files in os.walk(directory):
                    for file in files:
                        file_path = os.path.join(root, file)
                        try:
                            os.remove(file_path)
                            deleted_count += 1
                        except Exception as e:
                            print(f"[DEBUG] Failed to delete {file_path}: {e}")
                            deletion_summary["errors"].append(str(e))
                deletion_summary["deleted_count"] = deleted_count
            else:
                print(f"[DEBUG] Directory does not exist: {directory}")
        except Exception as e:
            print(f"[DEBUG] Local deletion failed: {e}")
            deletion_summary["errors"].append(f"Local deletion failed: {e}")

    return deletion_summary
