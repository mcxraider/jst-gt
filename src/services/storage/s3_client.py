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
    Create and return a cached S3 client.

    In Kubernetes environments, uses service account authentication.
    In local development, can use AWS_PROFILE if available.

    Raises:
        S3Error: If client creation fails or credentials are unavailable.
    """
    try:
        # Check if running in Kubernetes with service account
        aws_profile = os.environ.get("AWS_PROFILE")

        if aws_profile and aws_profile.strip():
            # Local development with AWS profile
            logger.info("Using AWS profile: %s", aws_profile)
            session = boto3.Session(profile_name=aws_profile)
            s3 = session.client(
                "s3",
                region_name=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", AWS_REGION),
            )
            logger.info("S3 client initialized successfully with profile '%s'", aws_profile)
        else:
            # Kubernetes or other environments - use default credential chain
            logger.info("Using default AWS credential chain (service account/IAM role)")
            s3 = boto3.client(
                "s3",
                region_name=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", AWS_REGION),
            )
            logger.info("S3 client initialized successfully with default credentials")

        return s3

    except ProfileNotFound as e:
        profile_name = os.environ.get("AWS_PROFILE", "unknown")
        raise S3Error(f"AWS profile not found: {profile_name}") from e

    except NoCredentialsError as e:
        raise S3Error("Unable to locate AWS credentials. Ensure service account is properly configured or AWS_PROFILE is set.") from e

    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        raise S3Error(f"Failed to create S3 client ({code}): {e}") from e

    except Exception as e:
        raise S3Error(f"Unexpected error creating S3 client: {e}") from e


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
    print(s3_path_clean)
    if "/" not in s3_path_clean:
        raise ValidationError("S3 path must contain both bucket and key")
    bucket, key = s3_path_clean.split("/", 1)
    return bucket, key


def validate_file_size(size_bytes, max_size_mb=100):
    """Validate file size constraints."""
    max_size_bytes = max_size_mb * 1024 * 1024
    if size_bytes > max_size_bytes:
        raise ValidationError(
            f"File size ({size_bytes} bytes) exceeds limit ({max_size_mb}MB)"
        )
