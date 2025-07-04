# services/storage/parquet_operations.py
"""
Parquet file operations for both local filesystem and S3 storage.
Handles saving, loading, and listing Parquet files for high-performance data I/O.
"""
import io
import pandas as pd
from pathlib import Path
import logging
import signal
from contextlib import contextmanager

from config import USE_S3
from .s3_client import get_s3_client, parse_s3_path, S3_BUCKET_NAME
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3TimeoutError(Exception):
    """Custom exception for S3 operation timeouts."""

    pass


@contextmanager
def timeout_handler(seconds=60, error_message="Operation timed out"):
    """
    Context manager to handle operation timeouts.

    Args:
        seconds (int): Timeout duration in seconds
        error_message (str): Error message to raise on timeout

    Raises:
        S3TimeoutError: If operation exceeds timeout duration
    """

    def timeout_signal_handler(signum, frame):
        raise S3TimeoutError(error_message)

    # Set up the signal handler
    old_handler = signal.signal(signal.SIGALRM, timeout_signal_handler)
    signal.alarm(seconds)

    try:
        yield
    finally:
        # Clean up
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def save_parquet(df, path, compression="snappy"):
    """
    Save DataFrame as Parquet file to local filesystem or S3.

    Args:
        df (pd.DataFrame): DataFrame to save
        path (str): File path for saving
        compression (str): Compression algorithm ('snappy', 'gzip', 'brotli', 'lz4', None)

    Raises:
        ValueError: If DataFrame is empty or path is invalid
        ClientError: If S3 upload fails
        IOError: If local file write fails
        S3TimeoutError: If S3 operation times out after 1 minute
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
                    f"📤 PARQUET S3 UPLOAD: Parsed path - Bucket: {S3_BUCKET_NAME}, Key: {key}"
                )
            else:
                # If path doesn't start with s3://, treat it as a key
                key = str(path).lstrip("/")
                logger.info(
                    f"📤 PARQUET S3 UPLOAD: Using path as key - Bucket: {S3_BUCKET_NAME}, Key: {key}"
                )

            logger.info(
                f"📊 DataFrame info: Shape {df.shape}, Size: {df.memory_usage(deep=True).sum()} bytes"
            )

            # Create parquet buffer
            parquet_buffer = io.BytesIO()
            df.to_parquet(
                parquet_buffer, index=False, compression=compression, engine="pyarrow"
            )
            parquet_size = len(parquet_buffer.getvalue())
            logger.info(
                f"📄 Parquet buffer size: {parquet_size} bytes (compression: {compression})"
            )

            logger.info(f"🚀 Starting S3 upload to s3://{S3_BUCKET_NAME}/{key}")
            with timeout_handler(
                60,
                "S3 parquet upload timed out after 1 minute. This may indicate permission issues or network problems.",
            ):
                get_s3_client().put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=key,
                    Body=parquet_buffer.getvalue(),
                    ContentType="application/octet-stream",
                )
            logger.info(
                f"✅ Parquet uploaded successfully to s3://{S3_BUCKET_NAME}/{key}"
            )
        except S3TimeoutError as e:
            logger.error(f"⏰ S3 TIMEOUT ERROR during Parquet upload: {e}")
            raise Exception(f"S3 upload timed out: {e}")
        except ClientError as e:
            logger.error(f"❌ S3 CLIENT ERROR during Parquet upload: {e}")
            raise Exception(f"Failed to upload Parquet to S3: {e}")
        except Exception as e:
            logger.error(f"❌ UNEXPECTED ERROR during Parquet upload: {e}")
            raise Exception(f"Unexpected error saving Parquet to S3: {e}")
    else:
        try:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(path, index=False, compression=compression, engine="pyarrow")
            logger.info(
                f"✅ Parquet saved locally: {path} (compression: {compression})"
            )
        except IOError as e:
            raise IOError(f"Failed to save Parquet locally: {e}")


def load_parquet(path, columns=None):
    """
    Load Parquet file from local filesystem or S3.

    Args:
        path (str): File path to load from
        columns (list, optional): Specific columns to load. Defaults to None (all columns).

    Returns:
        pd.DataFrame: Loaded DataFrame

    Raises:
        FileNotFoundError: If file doesn't exist
        ClientError: If S3 download fails
        Exception: If Parquet loading fails
        S3TimeoutError: If S3 operation times out after 1 minute
    """
    if USE_S3:
        try:
            with timeout_handler(
                60,
                "S3 parquet download timed out after 1 minute. This may indicate permission issues or network problems.",
            ):
                # Use hardcoded bucket name and extract just the key from the path
                if str(path).startswith("s3://"):
                    _, key = parse_s3_path(str(path))
                    logger.info(
                        f"📥 PARQUET S3 DOWNLOAD: Parsed path - Bucket: {S3_BUCKET_NAME}, Key: {key}"
                    )
                else:
                    # If path doesn't start with s3://, treat it as a key
                    key = str(path).lstrip("/")
                    logger.info(
                        f"📥 PARQUET S3 DOWNLOAD: Using path as key - Bucket: {S3_BUCKET_NAME}, Key: {key}"
                    )

                s3_client = get_s3_client()

                # Check if object exists
                try:
                    head_response = s3_client.head_object(
                        Bucket=S3_BUCKET_NAME, Key=key
                    )
                    file_size = head_response.get("ContentLength", 0)
                    logger.info(f"📄 Found Parquet file: Size {file_size} bytes")
                except ClientError as e:
                    if e.response["Error"]["Code"] == "404":
                        logger.error(
                            f"❌ Parquet file not found: s3://{S3_BUCKET_NAME}/{key}"
                        )
                        raise FileNotFoundError(
                            f"S3 object not found: s3://{S3_BUCKET_NAME}/{key}"
                        )
                    logger.error(f"❌ Error checking Parquet file: {e}")
                    raise

                logger.info(f"📥 Starting S3 download from s3://{S3_BUCKET_NAME}/{key}")
                obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
                df = pd.read_parquet(
                    io.BytesIO(obj["Body"].read()), columns=columns, engine="pyarrow"
                )
                logger.info(f"✅ Parquet loaded successfully: Shape {df.shape}")
                return df

        except S3TimeoutError as e:
            logger.error(f"⏰ S3 TIMEOUT ERROR during Parquet download: {e}")
            raise Exception(f"S3 download timed out: {e}")
        except ClientError as e:
            logger.error(f"❌ S3 CLIENT ERROR during Parquet download: {e}")
            raise Exception(f"Failed to download Parquet from S3: {e}")
        except Exception as e:
            logger.error(f"❌ UNEXPECTED ERROR during Parquet download: {e}")
            raise Exception(f"Unexpected error loading Parquet from S3: {e}")
    else:
        if not Path(path).exists():
            raise FileNotFoundError(f"Local file not found: {path}")
        try:
            df = pd.read_parquet(path, columns=columns, engine="pyarrow")
            logger.info(f"✅ Parquet loaded locally: Shape {df.shape}")
            return df
        except Exception as e:
            logger.error(f"❌ Error loading local Parquet file: {e}")
            raise Exception(f"Failed to load Parquet file: {e}")


def get_parquet_file_info(path):
    """
    Get metadata information about a Parquet file without loading it fully.

    Args:
        path (str): File path to inspect

    Returns:
        dict: Metadata including columns, row count, and file size

    Raises:
        FileNotFoundError: If file doesn't exist
        Exception: If metadata reading fails
        S3TimeoutError: If S3 operation times out after 1 minute
    """
    try:
        if USE_S3:
            with timeout_handler(
                60,
                "S3 parquet metadata read timed out after 1 minute. This may indicate permission issues or network problems.",
            ):
                if str(path).startswith("s3://"):
                    _, key = parse_s3_path(str(path))
                else:
                    key = str(path).lstrip("/")

                logger.info(
                    f"📄 Reading Parquet metadata from s3://{S3_BUCKET_NAME}/{key}"
                )
                s3_client = get_s3_client()
                obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
                parquet_file = io.BytesIO(obj["Body"].read())
        else:
            parquet_file = path

        # Use pyarrow to read metadata efficiently
        import pyarrow.parquet as pq

        parquet_metadata = pq.read_metadata(parquet_file)
        schema = pq.read_schema(parquet_file)

        return {
            "num_rows": parquet_metadata.num_rows,
            "num_columns": len(schema.names),
            "column_names": schema.names,
            "file_size_bytes": parquet_metadata.serialized_size,
            "compression": (
                str(parquet_metadata.row_group(0).column(0).compression)
                if parquet_metadata.num_row_groups > 0
                else "UNCOMPRESSED"
            ),
        }

    except S3TimeoutError as e:
        logger.error(f"⏰ S3 TIMEOUT ERROR during Parquet metadata read: {e}")
        raise Exception(f"S3 metadata read timed out: {e}")
    except Exception as e:
        logger.error(f"❌ Error reading Parquet metadata: {e}")
        raise Exception(f"Failed to read Parquet metadata: {e}")
