# services/storage/file_management.py
"""
File management operations for both local filesystem and S3 storage.
Handles listing files and directory cleanup.
"""
from pathlib import Path
import os
import logging

from config import USE_S3
from .s3_client import get_s3_client, parse_s3_path, S3_BUCKET_NAME
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


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
            # Use hardcoded bucket name and extract just the prefix from the path
            if str(directory).startswith("s3://"):
                _, prefix = parse_s3_path(str(directory))
            else:
                # If path doesn't start with s3://, treat it as a prefix
                prefix = str(directory).lstrip("/")

            s3 = get_s3_client()
            paginator = s3.get_paginator("list_objects_v2")
            file_list = []

            # Convert glob pattern to simple endswith check for S3
            if pattern != "*":
                suffix = pattern.replace("*", "")
            else:
                suffix = ""

            for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if not suffix or obj["Key"].endswith(suffix):
                        file_list.append(f"s3://{S3_BUCKET_NAME}/{obj['Key']}")
            return file_list

        except ClientError as e:
            raise Exception(f"Failed to list S3 objects: {e}")
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
        # Use hardcoded bucket name and extract just the prefix from the path
        if str(directory).startswith("s3://"):
            _, prefix = parse_s3_path(str(directory))
        else:
            # If path doesn't start with s3://, treat it as a prefix
            prefix = str(directory).lstrip("/")

        s3 = get_s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        matches = []
        for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                filename = key.split("/")[-1]
                if contains_string in filename and filename.endswith(file_ext):
                    matches.append(f"s3://{S3_BUCKET_NAME}/{key}")
        return matches
    except ClientError as e:
        raise Exception(f"Failed to list S3 objects in {directory}: {e}")
    except Exception as e:
        raise Exception(f"Unexpected error listing S3 files in {directory}: {e}")


def delete_all(directory):
    """
    Delete all files in a given S3 prefix or local directory, but preserve the folder marker (if any) for S3.

    Safety feature: For S3, only allows deletion in specific allowed prefixes to prevent accidental data loss.

    Args:
        directory (str): S3 directory/prefix path (e.g., 's3://bucket/prefix/')
                         or local directory path

    Returns:
        dict: Summary of deletion operation
    """
    deletion_summary = {"deleted_count": 0, "errors": []}

    # Define allowed S3 prefixes for deletion
    ALLOWED_S3_PREFIXES = [
        "s3_checkpoint",
        "s3_input",
        "s3_intermediate",
        "s3_misc_output",
        "s3_output",
    ]

    if USE_S3:
        try:
            logger.info(f"🗑️  S3 DELETION INITIATED for directory: {directory}")
            logger.info(f"🎯 Target Bucket: {S3_BUCKET_NAME}")
            print(f"\n[DEBUG] Starting deletion in: {directory}")
            # Use hardcoded bucket name and extract just the prefix from the path
            if str(directory).startswith("s3://"):
                _, prefix = parse_s3_path(str(directory))
                logger.info(
                    f"📁 Parsed S3 path - Bucket: {S3_BUCKET_NAME}, Prefix: {prefix}"
                )
            else:
                # If path doesn't start with s3://, treat it as a prefix
                prefix = str(directory).lstrip("/")
                logger.info(f"📁 Using directory as prefix: {prefix}")

            # Safety check: Only allow deletion in specific prefixes
            prefix_normalized = prefix.lstrip("/").rstrip("/")

            logger.info(f"🔍 SAFETY CHECK: Validating prefix '{prefix_normalized}'")
            logger.info(f"🛡️  Allowed prefixes: {ALLOWED_S3_PREFIXES}")

            # Check if the prefix starts with any of the allowed prefixes
            is_allowed = any(
                prefix_normalized.startswith(allowed_prefix)
                for allowed_prefix in ALLOWED_S3_PREFIXES
            )

            if not is_allowed:
                error_msg = (
                    f"Deletion not allowed for prefix '{prefix_normalized}'. "
                    f"Only allowed prefixes: {', '.join(ALLOWED_S3_PREFIXES)}"
                )
                logger.error(f"❌ SAFETY CHECK FAILED: {error_msg}")
                print(f"[DEBUG] {error_msg}")
                deletion_summary["errors"].append(error_msg)
                return deletion_summary

            logger.info(
                f"✅ SAFETY CHECK PASSED: Deletion allowed for prefix '{prefix_normalized}'"
            )
            print(f"[DEBUG] Deletion allowed for prefix: {prefix_normalized}")

            # Normalize prefix: remove leading slash, ensure trailing slash
            prefix = prefix.lstrip("/")
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            logger.info(f"🎯 Final S3 operation details:")
            logger.info(f"   📦 Bucket: {S3_BUCKET_NAME}")
            logger.info(f"   📁 Prefix: {prefix}")
            print(f"[DEBUG] Bucket: {S3_BUCKET_NAME}")
            print(f"[DEBUG] Prefix: {prefix}")
            s3 = get_s3_client()
            paginator = s3.get_paginator("list_objects_v2")
            delete_batch = []
            total_deleted = 0

            logger.info("🔍 Starting S3 object enumeration...")
            for page_num, page in enumerate(
                paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix)
            ):
                logger.info(f"📄 Processing page {page_num + 1}")
                print(f"\n[DEBUG] Processing page {page_num + 1}")
                objects = page.get("Contents", [])
                logger.info(f"📋 Found {len(objects)} objects in this page")
                print(f"[DEBUG] Number of objects in this page: {len(objects)}")
                if not objects:
                    continue
                for obj in objects:
                    key = obj["Key"]
                    logger.debug(f"🔍 Examining key: {key}")
                    print(f"[DEBUG] Found key: {key}")
                    # Skip the folder marker (which is the prefix itself)
                    if key == prefix.rstrip("/"):
                        logger.debug(f"⏭️  Skipping folder marker: {key}")
                        print(f"[DEBUG] Skipping folder marker: {key}")
                        continue
                    # Also skip "pseudo-folder" markers for subfolders (e.g. key endswith "/" and has nothing after the slash)
                    if key.endswith("/") and key.count("/") == prefix.count("/"):
                        logger.debug(f"⏭️  Skipping pseudo-folder marker: {key}")
                        print(f"[DEBUG] Skipping pseudo-folder marker: {key}")
                        continue
                    logger.info(f"🗑️  Adding to deletion batch: {key}")
                    print(f"[DEBUG] Adding key to delete_batch: {key}")
                    delete_batch.append({"Key": key})
                    if len(delete_batch) == 1000:
                        logger.info(
                            f"🚀 Deleting batch of 1000 objects from s3://{S3_BUCKET_NAME}"
                        )
                        print(f"[DEBUG] Deleting batch of 1000 keys:")
                        for k in delete_batch:
                            print(f"  - {k['Key']}")
                        resp = s3.delete_objects(
                            Bucket=S3_BUCKET_NAME, Delete={"Objects": delete_batch}
                        )
                        batch_deleted = len(resp.get("Deleted", []))
                        total_deleted += batch_deleted
                        logger.info(
                            f"✅ Successfully deleted {batch_deleted} objects (Total: {total_deleted})"
                        )
                        print(f"[DEBUG] Deleted {batch_deleted} objects in this batch")
                        if "Errors" in resp:
                            logger.error(
                                f"❌ Errors in batch deletion: {resp['Errors']}"
                            )
                            print(f"[DEBUG] Errors in batch: {resp['Errors']}")
                            deletion_summary["errors"].extend(resp["Errors"])
                        delete_batch = []
                # Delete any remaining keys
                if delete_batch:
                    logger.info(
                        f"🚀 Deleting final batch of {len(delete_batch)} objects from s3://{S3_BUCKET_NAME}"
                    )
                    print(
                        f"[DEBUG] Deleting final batch of {len(delete_batch)} keys in this page:"
                    )
                    for k in delete_batch:
                        print(f"  - {k['Key']}")
                    resp = s3.delete_objects(
                        Bucket=S3_BUCKET_NAME, Delete={"Objects": delete_batch}
                    )
                    batch_deleted = len(resp.get("Deleted", []))
                    total_deleted += batch_deleted
                    logger.info(
                        f"✅ Successfully deleted {batch_deleted} objects (Final Total: {total_deleted})"
                    )
                    print(f"[DEBUG] Deleted {batch_deleted} objects in this batch")
                    if "Errors" in resp:
                        logger.error(
                            f"❌ Errors in final batch deletion: {resp['Errors']}"
                        )
                        print(f"[DEBUG] Errors in final batch: {resp['Errors']}")
                        deletion_summary["errors"].extend(resp["Errors"])
                    delete_batch = []

            logger.info(
                f"🎉 S3 DELETION COMPLETED - Total objects deleted: {total_deleted}"
            )
            logger.info(f"📦 Bucket: {S3_BUCKET_NAME}, Prefix: {prefix}")
            print(f"\n[DEBUG] Total deleted: {total_deleted}")
            deletion_summary["deleted_count"] = total_deleted
        except ClientError as e:
            logger.error(f"❌ S3 CLIENT ERROR during deletion: {e}")
            print(f"[DEBUG] S3 deletion failed: {e}")
            deletion_summary["errors"].append(f"S3 deletion failed: {e}")
        except Exception as e:
            logger.error(f"❌ UNEXPECTED ERROR during S3 deletion: {e}")
            print(f"[DEBUG] Unexpected S3 deletion error: {e}")
            deletion_summary["errors"].append(f"Unexpected S3 deletion error: {e}")

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
