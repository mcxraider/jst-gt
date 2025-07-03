# services/ingestion/input_processor.py
"""
Input file processor that handles Excel/CSV uploads and converts them to Parquet format
for internal processing. This maintains backward compatibility with existing upload formats
while optimizing internal data operations.
"""
import pandas as pd
import io
import logging
from pathlib import Path
from typing import Tuple, Optional

from services.storage import save_parquet
from utils.format_converter import convert_excel_to_parquet, convert_csv_to_parquet

logger = logging.getLogger(__name__)


def process_uploaded_file(uploaded_file, target_path: str) -> Tuple[pd.DataFrame, str]:
    """
    Process uploaded file (Excel/CSV) and convert to Parquet for internal storage.

    Args:
        uploaded_file: Streamlit uploaded file object
        target_path (str): Path where the Parquet file should be saved

    Returns:
        Tuple[pd.DataFrame, str]: (DataFrame, parquet_file_path)
    """
    try:
        file_ext = Path(uploaded_file.name).suffix.lower()
        logger.info(f"📤 Processing uploaded file: {uploaded_file.name} ({file_ext})")

        # Read the uploaded file into DataFrame
        if file_ext in [".xlsx", ".xls"]:
            df = pd.read_excel(uploaded_file)
        elif file_ext == ".csv":
            df = pd.read_csv(uploaded_file)
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")

        # Convert target path to .parquet extension
        base_name = Path(target_path).stem
        parquet_path = str(Path(target_path).parent / f"{base_name}.parquet")

        # Save as Parquet for internal processing
        save_parquet(df, parquet_path)

        logger.info(
            f"✅ Converted {uploaded_file.name} to Parquet: {parquet_path} (Shape: {df.shape})"
        )
        return df, parquet_path

    except Exception as e:
        logger.error(f"❌ Failed to process uploaded file {uploaded_file.name}: {e}")
        raise


def process_sfw_upload(
    uploaded_file, expected_sheet_name: str, target_path: str
) -> Tuple[pd.DataFrame, str]:
    """
    Process SFW file upload with specific sheet validation.

    Args:
        uploaded_file: Streamlit uploaded file object
        expected_sheet_name (str): Name of the expected sheet (e.g., "SFW_HR")
        target_path (str): Path where the Parquet file should be saved

    Returns:
        Tuple[pd.DataFrame, str]: (DataFrame, parquet_file_path)
    """
    try:
        file_ext = Path(uploaded_file.name).suffix.lower()
        logger.info(
            f"📤 Processing SFW file: {uploaded_file.name} (Expected sheet: {expected_sheet_name})"
        )

        # Read the specific sheet from Excel file
        if file_ext in [".xlsx", ".xls"]:
            df = pd.read_excel(uploaded_file, sheet_name=expected_sheet_name)
        elif file_ext == ".csv":
            # For CSV, assume it contains the data from the expected sheet
            df = pd.read_csv(uploaded_file)
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")

        # Convert target path to .parquet extension
        base_name = Path(target_path).stem
        parquet_path = str(Path(target_path).parent / f"{base_name}.parquet")

        # Save as Parquet for internal processing
        save_parquet(df, parquet_path)

        logger.info(
            f"✅ Converted SFW file to Parquet: {parquet_path} (Shape: {df.shape})"
        )
        return df, parquet_path

    except Exception as e:
        logger.error(f"❌ Failed to process SFW file {uploaded_file.name}: {e}")
        raise


def process_sector_upload(
    uploaded_file, expected_sheet_name: str, target_path: str
) -> Tuple[pd.DataFrame, str]:
    """
    Process sector file upload with specific sheet validation.

    Args:
        uploaded_file: Streamlit uploaded file object
        expected_sheet_name (str): Name of the expected sheet (e.g., "HR")
        target_path (str): Path where the Parquet file should be saved

    Returns:
        Tuple[pd.DataFrame, str]: (DataFrame, parquet_file_path)
    """
    try:
        file_ext = Path(uploaded_file.name).suffix.lower()
        logger.info(
            f"📤 Processing sector file: {uploaded_file.name} (Expected sheet: {expected_sheet_name})"
        )

        # Read the specific sheet from Excel file
        if file_ext in [".xlsx", ".xls"]:
            df = pd.read_excel(uploaded_file, sheet_name=expected_sheet_name)
        elif file_ext == ".csv":
            # For CSV, assume it contains the data from the expected sheet
            df = pd.read_csv(uploaded_file)
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")

        # Convert target path to .parquet extension
        base_name = Path(target_path).stem
        parquet_path = str(Path(target_path).parent / f"{base_name}.parquet")

        # Save as Parquet for internal processing
        save_parquet(df, parquet_path)

        logger.info(
            f"✅ Converted sector file to Parquet: {parquet_path} (Shape: {df.shape})"
        )
        return df, parquet_path

    except Exception as e:
        logger.error(f"❌ Failed to process sector file {uploaded_file.name}: {e}")
        raise


def validate_and_process_file(
    uploaded_file, file_type: str, expected_sheet: str, target_path: str
) -> Tuple[pd.DataFrame, str]:
    """
    Unified function to validate and process uploaded files.

    Args:
        uploaded_file: Streamlit uploaded file object
        file_type (str): Type of file ("sfw" or "sector")
        expected_sheet (str): Expected sheet name
        target_path (str): Target path for saving

    Returns:
        Tuple[pd.DataFrame, str]: (DataFrame, parquet_file_path)
    """
    if file_type.lower() == "sfw":
        return process_sfw_upload(uploaded_file, expected_sheet, target_path)
    elif file_type.lower() == "sector":
        return process_sector_upload(uploaded_file, expected_sheet, target_path)
    else:
        return process_uploaded_file(uploaded_file, target_path)


def get_file_info_summary(
    df: pd.DataFrame, original_name: str, parquet_path: str
) -> dict:
    """
    Get summary information about the processed file.

    Args:
        df (pd.DataFrame): Processed DataFrame
        original_name (str): Original filename
        parquet_path (str): Path to created Parquet file

    Returns:
        dict: Summary information
    """
    return {
        "original_filename": original_name,
        "parquet_path": parquet_path,
        "shape": df.shape,
        "columns": list(df.columns),
        "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 * 1024),
        "data_types": df.dtypes.to_dict(),
    }
