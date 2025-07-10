# src/utils/health_check.py
from openai import OpenAI
import os
import logging
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from services.storage.s3_client import get_s3_client
from config import S3_BUCKET_NAME
from exceptions.storage_exceptions import S3Error

load_dotenv()

# Configure logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def check_openai_api_health():
    """
    Performs a simple health check of the OpenAI API.
    Returns True if healthy, False otherwise.
    """
    logger.info("Performing OpenAI API health check...")
    try:
        api_key = os.getenv("API_KEY")
        if not api_key:
            logger.error(
                "OpenAI API health check failed: `API_KEY` environment variable is not set."
            )
            return False

        client = OpenAI(
            api_key=api_key,
            base_url="https://litellm.govtext.gov.sg/",
        )

        response = client.chat.completions.create(
            model="gpt-4o-prd-gcc2-lb",
            messages=[{"role": "user", "content": "Hello"}],
            max_tokens=5,
            timeout=10,
        )

        if response.choices and response.choices[0].message.content:
            logger.info("OpenAI API connection successful.")
            return True
        else:
            logger.error("OpenAI API health check failed: Received an empty response.")
            return False
    except Exception as e:
        logger.error(f"OpenAI API health check failed: {e}", exc_info=True)
        return False


def check_s3_health():
    """
    Performs a health check on the S3 bucket by writing and deleting a test object,
    mirroring the application's primary S3 operations.
    Returns True if healthy, False otherwise.
    """
    logger.info("Performing S3 bucket health check...")
    s3_bucket = S3_BUCKET_NAME

    if not s3_bucket:
        logger.error(
            "S3 health check failed: `S3_BUCKET_NAME` is not configured in s3_client.py."
        )
        return False

    try:
        s3_client = get_s3_client()
        # Use a unique key to avoid potential race conditions if multiple checks run
        test_object_key = f"health_check/health_check_{os.urandom(8).hex()}.tmp"

        # 1. Test Put Operation (similar to save_parquet)
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=test_object_key,
            Body=b"health_check",
            ServerSideEncryption="AES256",
        )
        logger.info(f"S3 PutObject to bucket '{s3_bucket}' successful.")

        # 2. Test Delete Operation
        s3_client.delete_object(Bucket=s3_bucket, Key=test_object_key)
        logger.info(f"S3 DeleteObject from bucket '{s3_bucket}' successful.")

        logger.info("S3 bucket operations (Put, Delete) are fully functional.")
        return True

    except S3Error as e:
        logger.error(f"S3 health check failed during client creation: {e}")
        return False
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == "AccessDenied":
            logger.error(
                f"S3 health check failed: Access Denied. Check IAM permissions for bucket '{s3_bucket}'."
            )
        else:
            logger.error(f"S3 health check failed with a client error: {e}")
        return False
    except Exception as e:
        logger.error(f"S3 health check failed with an unexpected error: {e}")
        return False
