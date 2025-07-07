# services/storage/pickle_operations.py
"""
Secure pickle operations with validation and error handling.
"""
import io
import pickle
import logging
from pathlib import Path
from typing import Any, Set

from config import USE_S3
from services.storage.s3_client import (
    get_s3_client,
    parse_s3_path,
    validate_file_size,
    S3_BUCKET_NAME,
)
from exceptions.storage_exceptions import S3Error, LocalStorageError, ValidationError
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)

# Allowlist of safe types for pickle loading
SAFE_PICKLE_TYPES: Set[str] = {
    "builtins.dict",
    "builtins.list",
    "builtins.tuple",
    "builtins.str",
    "builtins.int",
    "builtins.float",
    "builtins.bool",
    "builtins.NoneType",
    "pandas.core.frame.DataFrame",
    "pandas.core.series.Series",
}


class SafeUnpickler(pickle.Unpickler):
    """Custom unpickler that only allows safe types."""

    def find_class(self, module, name):
        full_name = f"{module}.{name}"
        if full_name in SAFE_PICKLE_TYPES:
            return super().find_class(module, name)
        raise ValidationError(f"Unsafe pickle type: {full_name}")


def save_pickle(obj: Any, path: str, max_size_mb: int = 100) -> None:
    """
    Save Python object as pickle file with security validations.

    Args:
        obj: Python object to save
        path (str): File path for saving
        max_size_mb (int): Maximum file size in MB

    Raises:
        ValidationError: If object is invalid or too large
        S3Error: If S3 upload fails
        LocalStorageError: If local file write fails
    """
    if obj is None:
        raise ValidationError("Cannot pickle None object")

    # Serialize to buffer first to check size
    try:
        buf = io.BytesIO()
        pickle.dump(obj, buf, protocol=pickle.HIGHEST_PROTOCOL)
        buf.seek(0)

        # Validate size
        validate_file_size(len(buf.getvalue()), max_size_mb)

    except Exception as e:
        raise ValidationError(f"Failed to serialize object: {e}")

    if USE_S3:
        try:
            # Use hardcoded bucket name and extract just the key from the path
            if path.startswith("s3://"):
                _, key = parse_s3_path(str(path))
            else:
                # If path doesn't start with s3://, treat it as a key
                key = str(path).lstrip("/")

            get_s3_client().put_object(
                Bucket=S3_BUCKET_NAME,
                Key=key,
                Body=buf.getvalue(),
                ContentType="application/octet-stream",
                ServerSideEncryption="AES256",
                Metadata={
                    "pickle-version": str(pickle.HIGHEST_PROTOCOL),
                    "content-type": "pickle",
                },
            )
            logger.info(f"Successfully saved pickle to S3: s3://{S3_BUCKET_NAME}/{key}")

        except Exception as e:
            raise S3Error(f"Failed to upload pickle to S3: {e}")
    else:
        try:
            file_path = Path(path)
            file_path.parent.mkdir(parents=True, exist_ok=True)

            with open(file_path, "wb") as f:
                f.write(buf.getvalue())
            logger.info(f"Successfully saved pickle locally: {path}")

        except Exception as e:
            raise LocalStorageError(f"Failed to save pickle locally: {e}")


def load_pickle(path: str, safe_mode: bool = True) -> Any:
    """
    Load Python object from pickle file with security validations.

    Args:
        path (str): File path to load from
        safe_mode (bool): Use safe unpickling (recommended)

    Returns:
        Any: Loaded Python object

    Raises:
        ValidationError: If file doesn't exist or is unsafe
        S3Error: If S3 download fails
        LocalStorageError: If local file read fails
    """
    if USE_S3:
        try:
            # Use hardcoded bucket name and extract just the key from the path
            if path.startswith("s3://"):
                _, key = parse_s3_path(str(path))
            else:
                # If path doesn't start with s3://, treat it as a key
                key = str(path).lstrip("/")

            s3_client = get_s3_client()

            # Check if object exists
            try:
                head_response = s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=key)
                # Validate file size before downloading
                file_size = head_response.get("ContentLength", 0)
                validate_file_size(file_size)

            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    raise ValidationError(
                        f"S3 object not found: s3://{S3_BUCKET_NAME}/{key}"
                    )
                raise S3Error(f"Failed to check S3 object: {e}")

            obj_response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
            data = obj_response["Body"].read()
            logger.info(
                f"Successfully downloaded pickle from S3: s3://{S3_BUCKET_NAME}/{key}"
            )

        except S3Error:
            raise
        except Exception as e:
            raise S3Error(f"Failed to download pickle from S3: {e}")
    else:
        file_path = Path(path)
        if not file_path.exists():
            raise ValidationError(f"Local file not found: {path}")

        try:
            # Check file size
            file_size = file_path.stat().st_size
            validate_file_size(file_size)

            with open(file_path, "rb") as f:
                data = f.read()
            logger.info(f"Successfully read pickle locally: {path}")

        except Exception as e:
            raise LocalStorageError(f"Failed to read pickle locally: {e}")

    # Unpickle with safety checks
    try:
        if safe_mode:
            unpickler = SafeUnpickler(io.BytesIO(data))
            return unpickler.load()
        else:
            logger.warning("Loading pickle in unsafe mode - security risk!")
            return pickle.load(io.BytesIO(data))

    except Exception as e:
        raise ValidationError(f"Failed to unpickle data: {e}")
