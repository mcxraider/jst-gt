# services/storage/parquet_operations.py
"""
Parquet file operations for both local filesystem and S3 storage.
Handles saving, loading, and listing Parquet files for high-performance data I/O.
"""
import io
import pandas as pd
from pathlib import Path
import logging

from config import USE_S3
from .s3_client import get_s3_client, parse_s3_path, S3_BUCKET_NAME
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


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
    """
    logger.info(f"🎯 SAVE_PARQUET: Starting save operation for path: {path}")
    logger.info(
        f"🔧 SAVE_PARQUET: Configuration - USE_S3: {USE_S3}, Compression: {compression}"
    )

    # Validate DataFrame
    if df is None:
        logger.error("❌ SAVE_PARQUET: DataFrame is None")
        raise ValueError("DataFrame cannot be None")

    if (
        df.empty
        and "missing_content" not in str(path)
        and "poor_content" not in str(path)
    ):
        logger.error("❌ SAVE_PARQUET: DataFrame is empty (and not a special case)")
        raise ValueError("DataFrame cannot be empty")

    if df.empty:
        logger.warning(
            "⚠️ SAVE_PARQUET: DataFrame is empty but saving anyway (special case detected)"
        )

    logger.info(f"📊 SAVE_PARQUET: DataFrame validation passed - Shape: {df.shape}")

    if USE_S3:
        logger.info("☁️ SAVE_PARQUET: Using S3 storage mode")
        try:
            # Use hardcoded bucket name and extract just the key from the path
            if str(path).startswith("s3://"):
                _, key = parse_s3_path(str(path))
                logger.info(
                    f"📤 SAVE_PARQUET: Parsed S3 path - Bucket: {S3_BUCKET_NAME}, Key: {key}"
                )
            else:
                # If path doesn't start with s3://, treat it as a key
                key = str(path).lstrip("/")
                logger.info(
                    f"📤 SAVE_PARQUET: Using path as key - Bucket: {S3_BUCKET_NAME}, Key: {key}"
                )

            logger.info(
                f"📊 SAVE_PARQUET: DataFrame info - Shape: {df.shape}, Memory usage: {df.memory_usage(deep=True).sum()} bytes"
            )

            # Create parquet buffer
            logger.info(
                f"🔄 SAVE_PARQUET: Creating parquet buffer with {compression} compression"
            )
            parquet_buffer = io.BytesIO()
            df.to_parquet(
                parquet_buffer, index=False, compression=compression, engine="pyarrow"
            )
            parquet_size = len(parquet_buffer.getvalue())
            logger.info(
                f"📄 SAVE_PARQUET: Parquet buffer created - Size: {parquet_size} bytes (compression: {compression})"
            )

            logger.info(
                f"🚀 SAVE_PARQUET: Starting S3 upload to s3://{S3_BUCKET_NAME}/{key}"
            )
            get_s3_client().put_object(
                Bucket=S3_BUCKET_NAME,
                Key=key,
                Body=parquet_buffer.getvalue(),
                ContentType="application/octet-stream",
                ServerSideEncryption="AES256",
            )
            logger.info(
                f"✅ SAVE_PARQUET: S3 upload completed successfully to s3://{S3_BUCKET_NAME}/{key}"
            )
        except ClientError as e:
            logger.error(f"❌ SAVE_PARQUET: S3 CLIENT ERROR during upload: {e}")
            raise Exception(f"Failed to upload Parquet to S3: {e}")
        except Exception as e:
            logger.error(f"❌ SAVE_PARQUET: UNEXPECTED ERROR during S3 upload: {e}")
            raise Exception(f"Unexpected error saving Parquet to S3: {e}")
    else:
        logger.info("💾 SAVE_PARQUET: Using local filesystem mode")
        try:
            logger.info(f"📁 SAVE_PARQUET: Creating parent directories for: {path}")
            Path(path).parent.mkdir(parents=True, exist_ok=True)

            logger.info(f"💾 SAVE_PARQUET: Writing parquet file to local path: {path}")
            df.to_parquet(path, index=False, compression=compression, engine="pyarrow")
            logger.info(
                f"✅ SAVE_PARQUET: Local save completed successfully: {path} (compression: {compression})"
            )
        except IOError as e:
            logger.error(f"❌ SAVE_PARQUET: IO ERROR during local save: {e}")
            raise IOError(f"Failed to save Parquet locally: {e}")
        except Exception as e:
            logger.error(f"❌ SAVE_PARQUET: UNEXPECTED ERROR during local save: {e}")
            raise Exception(f"Unexpected error saving Parquet locally: {e}")


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
    """
    logger.info(f"📂 LOAD_PARQUET: Starting load operation for path: {path}")
    logger.info(f"🔧 LOAD_PARQUET: Configuration - USE_S3: {USE_S3}, Columns: {columns}")

    if USE_S3:
        logger.info("☁️ LOAD_PARQUET: Using S3 storage mode")
        try:
            # Use hardcoded bucket name and extract just the key from the path
            if str(path).startswith("s3://"):
                _, key = parse_s3_path(str(path))
                logger.info(
                    f"📥 LOAD_PARQUET: Parsed S3 path - Bucket: {S3_BUCKET_NAME}, Key: {key}"
                )
            else:
                # If path doesn't start with s3://, treat it as a key
                key = str(path).lstrip("/")
                logger.info(
                    f"📥 LOAD_PARQUET: Using path as key - Bucket: {S3_BUCKET_NAME}, Key: {key}"
                )

            logger.info(f"🔗 LOAD_PARQUET: Getting S3 client")
            s3_client = get_s3_client()

            # Check if object exists
            logger.info(f"🔍 LOAD_PARQUET: Checking if S3 object exists: {key}")
            try:
                head_response = s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=key)
                file_size = head_response.get("ContentLength", 0)
                logger.info(
                    f"📄 LOAD_PARQUET: Found Parquet file - Size: {file_size} bytes"
                )
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    logger.error(
                        f"❌ LOAD_PARQUET: File not found - s3://{S3_BUCKET_NAME}/{key}"
                    )
                    raise FileNotFoundError(
                        f"S3 object not found: s3://{S3_BUCKET_NAME}/{key}"
                    )
                logger.error(f"❌ LOAD_PARQUET: Error checking S3 object: {e}")
                raise

            logger.info(
                f"📥 LOAD_PARQUET: Starting S3 download from s3://{S3_BUCKET_NAME}/{key}"
            )
            obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)

            logger.info(f"🔄 LOAD_PARQUET: Processing downloaded data into DataFrame")
            df = pd.read_parquet(
                io.BytesIO(obj["Body"].read()), columns=columns, engine="pyarrow"
            )
            logger.info(
                f"✅ LOAD_PARQUET: S3 load completed successfully - Shape: {df.shape}"
            )
            return df

        except ClientError as e:
            logger.error(f"❌ LOAD_PARQUET: S3 CLIENT ERROR during download: {e}")
            raise Exception(f"Failed to download Parquet from S3: {e}")
        except Exception as e:
            logger.error(f"❌ LOAD_PARQUET: UNEXPECTED ERROR during S3 download: {e}")
            raise Exception(f"Unexpected error loading Parquet from S3: {e}")
    else:
        logger.info("💾 LOAD_PARQUET: Using local filesystem mode")
        logger.info(f"📁 LOAD_PARQUET: Checking if local file exists: {path}")
        if not Path(path).exists():
            logger.error(f"❌ LOAD_PARQUET: Local file not found: {path}")
            raise FileNotFoundError(f"Local file not found: {path}")

        try:
            logger.info(f"📖 LOAD_PARQUET: Reading parquet file from local path: {path}")
            df = pd.read_parquet(path, columns=columns, engine="pyarrow")
            logger.info(
                f"✅ LOAD_PARQUET: Local load completed successfully - Shape: {df.shape}"
            )
            return df
        except Exception as e:
            logger.error(f"❌ LOAD_PARQUET: ERROR during local file read: {e}")
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
    """
    logger.info(f"📊 GET_PARQUET_FILE_INFO: Starting metadata read for path: {path}")
    logger.info(f"🔧 GET_PARQUET_FILE_INFO: Configuration - USE_S3: {USE_S3}")

    try:
        if USE_S3:
            logger.info("☁️ GET_PARQUET_FILE_INFO: Using S3 storage mode")
            if str(path).startswith("s3://"):
                _, key = parse_s3_path(str(path))
                logger.info(
                    f"📥 GET_PARQUET_FILE_INFO: Parsed S3 path - Bucket: {S3_BUCKET_NAME}, Key: {key}"
                )
            else:
                key = str(path).lstrip("/")
                logger.info(
                    f"📥 GET_PARQUET_FILE_INFO: Using path as key - Bucket: {S3_BUCKET_NAME}, Key: {key}"
                )

            logger.info(
                f"📄 GET_PARQUET_FILE_INFO: Reading metadata from s3://{S3_BUCKET_NAME}/{key}"
            )
            s3_client = get_s3_client()

            logger.info(
                f"🔄 GET_PARQUET_FILE_INFO: Downloading S3 object for metadata analysis"
            )
            obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
            parquet_file = io.BytesIO(obj["Body"].read())
            logger.info(f"📦 GET_PARQUET_FILE_INFO: S3 object downloaded successfully")
        else:
            logger.info("💾 GET_PARQUET_FILE_INFO: Using local filesystem mode")
            logger.info(
                f"📁 GET_PARQUET_FILE_INFO: Reading metadata from local path: {path}"
            )
            parquet_file = path

        # Use pyarrow to read metadata efficiently
        logger.info(f"🔍 GET_PARQUET_FILE_INFO: Analyzing parquet metadata with pyarrow")
        import pyarrow.parquet as pq

        parquet_metadata = pq.read_metadata(parquet_file)
        schema = pq.read_schema(parquet_file)

        metadata_info = {
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

        logger.info(f"📊 GET_PARQUET_FILE_INFO: Metadata analysis completed")
        logger.info(
            f"📈 GET_PARQUET_FILE_INFO: Results - Rows: {metadata_info['num_rows']}, Columns: {metadata_info['num_columns']}, Size: {metadata_info['file_size_bytes']} bytes, Compression: {metadata_info['compression']}"
        )

        return metadata_info

    except Exception as e:
        logger.error(f"❌ GET_PARQUET_FILE_INFO: ERROR during metadata read: {e}")
        raise Exception(f"Failed to read Parquet metadata: {e}")
