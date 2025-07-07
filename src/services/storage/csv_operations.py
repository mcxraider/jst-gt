# services/storage/csv_operations.py
"""
CSV file operations for both local filesystem and S3 storage.
Handles saving, loading, and listing CSV files.
"""
import io
import pandas as pd
from pathlib import Path
import logging

from config import USE_S3
from .s3_client import get_s3_client, parse_s3_path, S3_BUCKET_NAME
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def save_csv(df, path):
    """
    Save DataFrame as CSV to local filesystem or S3.

    Args:
        df (pd.DataFrame): DataFrame to save
        path (str): File path for saving

    Raises:
        ValueError: If DataFrame is empty or path is invalid
        ClientError: If S3 upload fails
        IOError: If local file write fails
    """
    if (
        df is None
        or df.empty
        and "missing_content" not in str(path)
        and "poor_content" not in str(path)
    ):
        raise ValueError("DataFrame cannot be None or empty")

    if USE_S3:
        try:
            # Use hardcoded bucket name and extract just the key from the path
            if str(path).startswith("s3://"):
                _, key = parse_s3_path(str(path))
                logger.info(
                    f"📤 CSV S3 UPLOAD: Parsed path - Bucket: {S3_BUCKET_NAME}, Key: {key}"
                )
            else:
                # If path doesn't start with s3://, treat it as a key
                key = str(path).lstrip("/")
                logger.info(
                    f"📤 CSV S3 UPLOAD: Using path as key - Bucket: {S3_BUCKET_NAME}, Key: {key}"
                )

            logger.info(
                f"📊 DataFrame info: Shape {df.shape}, Size: {df.memory_usage(deep=True).sum()} bytes"
            )
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, encoding="utf-8")
            csv_size = len(csv_buffer.getvalue().encode("utf-8"))
            logger.info(f"📄 CSV buffer size: {csv_size} bytes")

            get_s3_client().put_object(
                Bucket=S3_BUCKET_NAME,
                Key=key,
                Body=csv_buffer.getvalue(),
                ContentType="text/csv",
                ServerSideEncryption="AES256",
            )
            logger.info(f"✅ CSV uploaded successfully to s3://{S3_BUCKET_NAME}/{key}")
        except ClientError as e:
            logger.error(f"❌ S3 CLIENT ERROR during CSV upload: {e}")
            raise Exception(f"Failed to upload CSV to S3: {e}")
        except Exception as e:
            logger.error(f"❌ UNEXPECTED ERROR during CSV upload: {e}")
            raise Exception(f"Unexpected error saving CSV to S3: {e}")
    else:
        try:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(path, index=False, encoding="utf-8")
        except IOError as e:
            raise IOError(f"Failed to save CSV locally: {e}")


def load_csv(path):
    """
    Load CSV file from local filesystem or S3.

    Args:
        path (str): File path to load from

    Returns:
        pd.DataFrame: Loaded DataFrame

    Raises:
        FileNotFoundError: If file doesn't exist
        ClientError: If S3 download fails
        pd.errors.EmptyDataError: If CSV is empty
    """
    if USE_S3:
        try:
            # Use hardcoded bucket name and extract just the key from the path
            if str(path).startswith("s3://"):
                _, key = parse_s3_path(str(path))
                logger.info(
                    f"📥 CSV S3 DOWNLOAD: Parsed path - Bucket: {S3_BUCKET_NAME}, Key: {key}"
                )
            else:
                # If path doesn't start with s3://, treat it as a key
                key = str(path).lstrip("/")
                logger.info(
                    f"📥 CSV S3 DOWNLOAD: Using path as key - Bucket: {S3_BUCKET_NAME}, Key: {key}"
                )

            s3_client = get_s3_client()

            # Check if object exists
            try:
                head_response = s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=key)
                file_size = head_response.get("ContentLength", 0)
                logger.info(f"📄 Found CSV file: Size {file_size} bytes")
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    logger.error(f"❌ CSV file not found: s3://{S3_BUCKET_NAME}/{key}")
                    raise FileNotFoundError(
                        f"S3 object not found: s3://{S3_BUCKET_NAME}/{key}"
                    )
                logger.error(f"❌ Error checking CSV file: {e}")
                raise

            logger.info(f"📥 Downloading CSV from s3://{S3_BUCKET_NAME}/{key}")
            obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
            df = pd.read_csv(io.BytesIO(obj["Body"].read()), encoding="utf-8")
            logger.info(f"✅ CSV loaded successfully: Shape {df.shape}")
            return df

        except ClientError as e:
            logger.error(f"❌ S3 CLIENT ERROR during CSV download: {e}")
            raise Exception(f"Failed to download CSV from S3: {e}")
        except pd.errors.EmptyDataError:
            logger.error(f"❌ CSV file is empty: {path}")
            raise pd.errors.EmptyDataError(f"CSV file is empty: {path}")
        except Exception as e:
            logger.error(f"❌ UNEXPECTED ERROR during CSV download: {e}")
            raise Exception(f"Unexpected error loading CSV from S3: {e}")
    else:
        if not Path(path).exists():
            raise FileNotFoundError(f"Local file not found: {path}")
        try:
            return pd.read_csv(path, encoding="utf-8")
        except pd.errors.EmptyDataError:
            raise pd.errors.EmptyDataError(f"CSV file is empty: {path}")
