# services/storage/s3_client.py
"""
S3 client configuration and utilities with improved error handling.
"""
import os
import logging
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv, find_dotenv
from functools import lru_cache

from config import S3_BUCKET_NAME, AWS_REGION
from exceptions.storage_exceptions import S3Error, ValidationError

# Load environment variables
dotenv_path = find_dotenv('.env.default', usecwd=True) or '.env.default'
load_dotenv(dotenv_path, override=True)

# Configure logging
logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_s3_client():
    """
    Create and return a cached S3 client with configured credentials.

    Returns:
        boto3.client: Configured S3 client

    Raises:
        S3Error: If credentials are missing or invalid
    """
    try:
        aws_access_key = os.environ["AWS_ACCESS_KEY_ID"]
        aws_secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]

        if not aws_access_key or not aws_secret_key:
            raise S3Error("AWS credentials cannot be empty")

    except KeyError as e:
        raise S3Error(f"Missing required environment variable: {e}")

    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=os.environ.get("AWS_REGION", AWS_REGION),
        )

        # Remove: client.list_buckets()
        logger.info("S3 client initialized successfully")
        return client

    except NoCredentialsError:
        raise S3Error("Invalid AWS credentials provided")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        raise S3Error(f"Failed to create S3 client ({error_code}): {e}")


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
