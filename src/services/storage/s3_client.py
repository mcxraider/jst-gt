# services/storage/s3_client.py
"""
S3 client configuration and utilities with improved error handling.

This module provides S3 client functionality for Kubernetes environments with:
- Kubernetes service account authentication (service account: ns-writer)
- Comprehensive permission checking and logging
- Automatic credential handling via AWS credential chain
"""
import os
import logging
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, ProfileNotFound
from dotenv import load_dotenv
from functools import lru_cache

from config import AWS_REGION
from exceptions.storage_exceptions import S3Error, ValidationError


load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Hardcoded bucket name for now
S3_BUCKET_NAME = "t-gen-stg-ssg-test-s3"

# Kubernetes service account configuration
K8S_SERVICE_ACCOUNT_NAME = "ns-writer"


def check_s3_permissions(s3_client, bucket_name):
    """
    Check and log available S3 permissions for the current credentials.

    Args:
        s3_client: Boto3 S3 client instance
        bucket_name: Name of the S3 bucket to test permissions on
    """
    logger.info("🔍 Checking S3 permissions for bucket: %s", bucket_name)

    permissions = {
        "list_bucket": False,
        "get_object": False,
        "put_object": False,
        "delete_object": False,
        "head_object": False,
        "get_bucket_location": False,
        "get_bucket_versioning": False,
    }

    # Test ListBucket permission
    try:
        s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
        permissions["list_bucket"] = True
        logger.info("✅ ListBucket: ALLOWED")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "AccessDenied":
            logger.warning("❌ ListBucket: DENIED")
        else:
            logger.warning("❌ ListBucket: ERROR (%s)", error_code)

    # Test GetObject permission (try to get a non-existent object)
    try:
        s3_client.head_object(Bucket=bucket_name, Key="permission-test-key")
        permissions["head_object"] = True
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "404":  # Not found is good - means we have permission
            permissions["get_object"] = True
            permissions["head_object"] = True
            logger.info("✅ GetObject/HeadObject: ALLOWED")
        elif error_code == "AccessDenied":
            logger.warning("❌ GetObject/HeadObject: DENIED")
        else:
            logger.warning("❌ GetObject/HeadObject: ERROR (%s)", error_code)

    # Test PutObject permission (try to put a small test object)
    try:
        test_key = "permission-test/test-file.txt"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=b"test",
            ContentType="text/plain",
            ServerSideEncryption="AES256",
        )
        permissions["put_object"] = True
        logger.info("✅ PutObject: ALLOWED")

        # Clean up test object
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=test_key)
            permissions["delete_object"] = True
            logger.info("✅ DeleteObject: ALLOWED")
        except ClientError:
            logger.warning("❌ DeleteObject: DENIED (test file may remain)")

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "AccessDenied":
            logger.warning("❌ PutObject: DENIED")
        else:
            logger.warning("❌ PutObject: ERROR (%s)", error_code)

    # Test GetBucketLocation permission
    try:
        response = s3_client.get_bucket_location(Bucket=bucket_name)
        permissions["get_bucket_location"] = True
        location = response.get("LocationConstraint") or "us-east-1"
        logger.info("✅ GetBucketLocation: ALLOWED (Region: %s)", location)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "AccessDenied":
            logger.warning("❌ GetBucketLocation: DENIED")
        else:
            logger.warning("❌ GetBucketLocation: ERROR (%s)", error_code)

    # Test GetBucketVersioning permission
    try:
        s3_client.get_bucket_versioning(Bucket=bucket_name)
        permissions["get_bucket_versioning"] = True
        logger.info("✅ GetBucketVersioning: ALLOWED")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "AccessDenied":
            logger.warning("❌ GetBucketVersioning: DENIED")
        else:
            logger.warning("❌ GetBucketVersioning: ERROR (%s)", error_code)

    # Summary
    allowed_permissions = [k for k, v in permissions.items() if v]
    denied_permissions = [k for k, v in permissions.items() if not v]

    logger.info("📊 Permission Summary:")
    logger.info(
        "   ✅ Allowed: %s",
        ", ".join(allowed_permissions) if allowed_permissions else "None",
    )
    logger.info(
        "   ❌ Denied: %s",
        ", ".join(denied_permissions) if denied_permissions else "None",
    )

    return permissions


def get_caller_identity(s3_client):
    """
    Get and log the AWS caller identity (user/role information).

    Args:
        s3_client: Boto3 S3 client instance
    """
    try:
        # Use STS to get caller identity
        sts_client = boto3.client("sts", region_name=s3_client.meta.region_name)
        identity = sts_client.get_caller_identity()

        logger.info("👤 AWS Caller Identity:")
        logger.info("   🆔 User ID: %s", identity.get("UserId", "Unknown"))
        logger.info("   👤 ARN: %s", identity.get("Arn", "Unknown"))
        logger.info("   🏢 Account: %s", identity.get("Account", "Unknown"))

        # Determine if it's a user or role
        arn = identity.get("Arn", "")
        if ":user/" in arn:
            logger.info("   🔑 Type: IAM User")
        elif ":role/" in arn:
            logger.info("   🔑 Type: IAM Role")
            # Check if it's a service account role
            if K8S_SERVICE_ACCOUNT_NAME in arn:
                logger.info(
                    "   🚀 Kubernetes Service Account: %s", K8S_SERVICE_ACCOUNT_NAME
                )
        elif ":assumed-role/" in arn:
            logger.info("   🔑 Type: Assumed Role")
            # Check if it's a service account assumed role
            if K8S_SERVICE_ACCOUNT_NAME in arn:
                logger.info(
                    "   🚀 Kubernetes Service Account: %s", K8S_SERVICE_ACCOUNT_NAME
                )
        else:
            logger.info("   🔑 Type: Unknown")

    except ClientError as e:
        logger.warning("❌ Could not get caller identity: %s", e)
    except Exception as e:
        logger.warning("❌ Unexpected error getting caller identity: %s", e)


@lru_cache(maxsize=1)
def get_s3_client():
    """
    Create and return a cached S3 client.

    Uses Kubernetes service account authentication with the configured service account.

    Raises:
        S3Error: If client creation fails or credentials are unavailable.
    """
    try:
        # Log Kubernetes service account usage
        logger.info("🚀 Running in Kubernetes environment")
        logger.info("🔧 Using service account: %s", K8S_SERVICE_ACCOUNT_NAME)
        logger.info("🔧 Using default AWS credential chain (service account/IAM role)")

        # Create S3 client using default credential chain
        s3 = boto3.client(
            "s3",
            region_name=os.environ.get("AWS_REGION")
            or os.environ.get("AWS_DEFAULT_REGION", AWS_REGION),
        )

        logger.info(
            "✅ S3 client initialized successfully with Kubernetes service account"
        )

        # Get caller identity information
        get_caller_identity(s3)

        # Check permissions for the configured bucket
        check_s3_permissions(s3, S3_BUCKET_NAME)

        return s3

    except NoCredentialsError as e:
        error_msg = (
            f"Unable to locate AWS credentials. "
            f"Ensure service account '{K8S_SERVICE_ACCOUNT_NAME}' is properly configured with IAM role."
        )
        raise S3Error(error_msg) from e

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
    logger.debug("Parsing S3 path: %s", s3_path_clean)

    if "/" not in s3_path_clean:
        raise ValidationError("S3 path must contain both bucket and key")

    bucket, key = s3_path_clean.split("/", 1)

    if not bucket:
        raise ValidationError(
            f"Empty bucket name in S3 path: {s3_path}. "
            "Check that S3_BUCKET_NAME environment variable is properly set."
        )

    return bucket, key


def validate_file_size(size_bytes, max_size_mb=100):
    """Validate file size constraints."""
    max_size_bytes = max_size_mb * 1024 * 1024
    if size_bytes > max_size_bytes:
        raise ValidationError(
            f"File size ({size_bytes} bytes) exceeds limit ({max_size_mb}MB)"
        )


def get_s3_config_info():
    """
    Get S3 client configuration information for debugging and monitoring.

    Returns:
        dict: Configuration information including environment, service account, and bucket details
    """
    config_info = {
        "environment": "kubernetes",
        "service_account_name": K8S_SERVICE_ACCOUNT_NAME,
        "bucket_name": S3_BUCKET_NAME,
        "aws_region": os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION", AWS_REGION),
        "k8s_namespace": os.environ.get("KUBERNETES_NAMESPACE"),
        "k8s_pod_name": os.environ.get("HOSTNAME"),
        "k8s_service_host": os.environ.get("KUBERNETES_SERVICE_HOST"),
    }

    return config_info
