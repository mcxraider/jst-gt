# services/storage/s3_client.py
"""
S3 client configuration and utilities with improved error handling.
"""
import os
import logging
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, ProfileNotFound
from dotenv import load_dotenv
from functools import lru_cache

from config import S3_BUCKET_NAME, AWS_REGION
from exceptions.storage_exceptions import S3Error, ValidationError


load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_s3_client():
    """
    Create and return a cached S3 client using the AWS_PROFILE SSO profile.

    Raises:
        S3Error: If profile is missing, not found, or client creation fails.
    """
    # 1) Pull your profile name (e.g. "ns-writer") from the env
    try:
        profile = os.environ["AWS_PROFILE"]
        if not profile.strip():
            raise S3Error("AWS_PROFILE cannot be empty")
    except KeyError:
        raise S3Error("Missing required environment variable: AWS_PROFILE")

    # 2) Build a Session with that profile
    try:
        session = boto3.Session(profile_name=profile)
        s3 = session.client(
            "s3",
            region_name=os.environ.get("AWS_REGION", AWS_REGION),
        )
        logger.info("S3 client initialized successfully with profile '%s'", profile)
        return s3

    except ProfileNotFound as e:
        raise S3Error(f"AWS profile not found: {profile}") from e

    except NoCredentialsError as e:
        raise S3Error(f"Invalid credentials for AWS profile '{profile}'") from e

    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        raise S3Error(f"Failed to create S3 client ({code}): {e}") from e


def parse_s3_path(s3_path):
    """
    Parse and validate S3 path into bucket and key components.

    Args:
        s3_path (str): S3 path in format "s3://bucket-name/key/path"

    Returns:
        tuple: (bucket_name, key_path)

    Raises:
        ValidationError: If path format is invalid
    """
    if not isinstance(s3_path, str):
        raise ValidationError("S3 path must be a string")

    if not s3_path.startswith("s3://"):
        raise ValidationError(
            "S3 path must start with 's3://'"
            + f", your path looks like this: {s3_path}"
        )

    s3_path_clean = s3_path.replace("s3://", "")

    if "/" not in s3_path_clean:
        raise ValidationError("S3 path must contain both bucket and key")

    bucket, key = s3_path_clean.split("/", 1)
    if not bucket:
        raise ValidationError("Bucket name cannot be empty")

    if key is None:
        key = ""

    # Basic bucket name validation (AWS rules are complex, this is simplified)
    if not bucket.replace("-", "").replace(".", "").isalnum():
        raise ValidationError("Invalid bucket name format")

    return bucket, key


def validate_file_size(size_bytes, max_size_mb=100):
    """Validate file size constraints."""
    max_size_bytes = max_size_mb * 1024 * 1024
    if size_bytes > max_size_bytes:
        raise ValidationError(
            f"File size ({size_bytes} bytes) exceeds limit ({max_size_mb}MB)"
        )
