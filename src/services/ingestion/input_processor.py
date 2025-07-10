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
    logger.info(f"🚀 PROCESS_UPLOADED_FILE: Starting file processing")
    logger.info(f"📁 PROCESS_UPLOADED_FILE: Target path: {target_path}")

    try:
        file_ext = Path(uploaded_file.name).suffix.lower()
        file_size = uploaded_file.size if hasattr(uploaded_file, "size") else "Unknown"
        logger.info(f"📤 PROCESS_UPLOADED_FILE: Processing file: {uploaded_file.name}")
        logger.info(
            f"📊 PROCESS_UPLOADED_FILE: File details - Extension: {file_ext}, Size: {file_size} bytes"
        )

        # Read the uploaded file into DataFrame
        logger.info(f"📖 PROCESS_UPLOADED_FILE: Reading file into DataFrame")
        if file_ext in [".xlsx", ".xls"]:
            logger.info(f"📊 PROCESS_UPLOADED_FILE: Reading Excel file")
            df = pd.read_excel(uploaded_file)
        elif file_ext == ".csv":
            logger.info(f"📋 PROCESS_UPLOADED_FILE: Reading CSV file")
            df = pd.read_csv(uploaded_file)
        else:
            error_msg = f"Unsupported file format: {file_ext}"
            logger.error(f"❌ PROCESS_UPLOADED_FILE: {error_msg}")
            raise ValueError(error_msg)

        logger.info(
            f"📈 PROCESS_UPLOADED_FILE: DataFrame loaded successfully - Shape: {df.shape}"
        )
        logger.info(f"📋 PROCESS_UPLOADED_FILE: DataFrame columns: {list(df.columns)}")
        logger.info(
            f"💾 PROCESS_UPLOADED_FILE: Memory usage: {df.memory_usage(deep=True).sum() / (1024 * 1024):.2f} MB"
        )

        # Convert target path to .parquet extension
        base_name = Path(target_path).stem
        parquet_path = str(Path(target_path).parent / f"{base_name}.parquet")
        logger.info(f"🔄 PROCESS_UPLOADED_FILE: Converting path to Parquet format")
        logger.info(f"📁 PROCESS_UPLOADED_FILE: Original target: {target_path}")
        logger.info(f"📁 PROCESS_UPLOADED_FILE: Parquet target: {parquet_path}")

        # Save as Parquet for internal processing
        logger.info(f"💾 PROCESS_UPLOADED_FILE: Saving DataFrame as Parquet")
        save_parquet(df, parquet_path)

        logger.info(
            f"✅ PROCESS_UPLOADED_FILE: Successfully converted {uploaded_file.name} to Parquet"
        )
        logger.info(f"📍 PROCESS_UPLOADED_FILE: Final location: {parquet_path}")
        logger.info(f"📊 PROCESS_UPLOADED_FILE: Final DataFrame shape: {df.shape}")

        return df, parquet_path

    except Exception as e:
        logger.error(
            f"❌ PROCESS_UPLOADED_FILE: Failed to process file {uploaded_file.name}"
        )
        logger.error(f"💥 PROCESS_UPLOADED_FILE: Error details: {str(e)}")
        logger.error(f"🔍 PROCESS_UPLOADED_FILE: Error type: {type(e).__name__}")
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
    logger.info(f"🚀 PROCESS_SFW_UPLOAD: Starting SFW file processing")
    logger.info(f"📁 PROCESS_SFW_UPLOAD: Target path: {target_path}")
    logger.info(f"📋 PROCESS_SFW_UPLOAD: Expected sheet: {expected_sheet_name}")

    try:
        file_ext = Path(uploaded_file.name).suffix.lower()
        file_size = uploaded_file.size if hasattr(uploaded_file, "size") else "Unknown"
        logger.info(f"📤 PROCESS_SFW_UPLOAD: Processing SFW file: {uploaded_file.name}")
        logger.info(
            f"📊 PROCESS_SFW_UPLOAD: File details - Extension: {file_ext}, Size: {file_size} bytes"
        )

        # Read the specific sheet from Excel file
        logger.info(f"📖 PROCESS_SFW_UPLOAD: Reading file with sheet validation")
        if file_ext in [".xlsx", ".xls"]:
            logger.info(
                f"📊 PROCESS_SFW_UPLOAD: Reading Excel sheet '{expected_sheet_name}'"
            )
            df = pd.read_excel(uploaded_file, sheet_name=expected_sheet_name)
        elif file_ext == ".csv":
            logger.info(
                f"📋 PROCESS_SFW_UPLOAD: Reading CSV file (assuming correct sheet data)"
            )
            # For CSV, assume it contains the data from the expected sheet
            df = pd.read_csv(uploaded_file)
        else:
            error_msg = f"Unsupported file format: {file_ext}"
            logger.error(f"❌ PROCESS_SFW_UPLOAD: {error_msg}")
            raise ValueError(error_msg)

        logger.info(
            f"📈 PROCESS_SFW_UPLOAD: SFW DataFrame loaded successfully - Shape: {df.shape}"
        )
        logger.info(f"📋 PROCESS_SFW_UPLOAD: DataFrame columns: {list(df.columns)}")
        logger.info(
            f"💾 PROCESS_SFW_UPLOAD: Memory usage: {df.memory_usage(deep=True).sum() / (1024 * 1024):.2f} MB"
        )

        # Convert target path to .parquet extension
        base_name = Path(target_path).stem
        parquet_path = str(Path(target_path).parent / f"{base_name}.parquet")
        logger.info(f"🔄 PROCESS_SFW_UPLOAD: Converting path to Parquet format")
        logger.info(f"📁 PROCESS_SFW_UPLOAD: Original target: {target_path}")
        logger.info(f"📁 PROCESS_SFW_UPLOAD: Parquet target: {parquet_path}")

        # Save as Parquet for internal processing
        logger.info(f"💾 PROCESS_SFW_UPLOAD: Saving SFW DataFrame as Parquet")
        save_parquet(df, parquet_path)

        logger.info(
            f"✅ PROCESS_SFW_UPLOAD: Successfully converted SFW file to Parquet"
        )
        logger.info(f"📍 PROCESS_SFW_UPLOAD: Final location: {parquet_path}")
        logger.info(f"📊 PROCESS_SFW_UPLOAD: Final DataFrame shape: {df.shape}")

        return df, parquet_path

    except Exception as e:
        logger.error(
            f"❌ PROCESS_SFW_UPLOAD: Failed to process SFW file {uploaded_file.name}"
        )
        logger.error(f"💥 PROCESS_SFW_UPLOAD: Error details: {str(e)}")
        logger.error(f"🔍 PROCESS_SFW_UPLOAD: Error type: {type(e).__name__}")
        if "sheet_name" in str(e).lower():
            logger.error(
                f"📋 PROCESS_SFW_UPLOAD: Sheet name issue - Expected: '{expected_sheet_name}'"
            )
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
    logger.info(f"🚀 PROCESS_SECTOR_UPLOAD: Starting sector file processing")
    logger.info(f"📁 PROCESS_SECTOR_UPLOAD: Target path: {target_path}")
    logger.info(f"📋 PROCESS_SECTOR_UPLOAD: Expected sheet: {expected_sheet_name}")

    try:
        file_ext = Path(uploaded_file.name).suffix.lower()
        file_size = uploaded_file.size if hasattr(uploaded_file, "size") else "Unknown"
        logger.info(
            f"📤 PROCESS_SECTOR_UPLOAD: Processing sector file: {uploaded_file.name}"
        )
        logger.info(
            f"📊 PROCESS_SECTOR_UPLOAD: File details - Extension: {file_ext}, Size: {file_size} bytes"
        )

        # Read the specific sheet from Excel file
        logger.info(f"📖 PROCESS_SECTOR_UPLOAD: Reading file with sheet validation")
        if file_ext in [".xlsx", ".xls"]:
            logger.info(
                f"📊 PROCESS_SECTOR_UPLOAD: Reading Excel sheet '{expected_sheet_name}'"
            )
            df = pd.read_excel(uploaded_file, sheet_name=expected_sheet_name)
        elif file_ext == ".csv":
            logger.info(
                f"📋 PROCESS_SECTOR_UPLOAD: Reading CSV file (assuming correct sheet data)"
            )
            # For CSV, assume it contains the data from the expected sheet
            df = pd.read_csv(uploaded_file)
        else:
            error_msg = f"Unsupported file format: {file_ext}"
            logger.error(f"❌ PROCESS_SECTOR_UPLOAD: {error_msg}")
            raise ValueError(error_msg)

        logger.info(
            f"📈 PROCESS_SECTOR_UPLOAD: Sector DataFrame loaded successfully - Shape: {df.shape}"
        )
        logger.info(f"📋 PROCESS_SECTOR_UPLOAD: DataFrame columns: {list(df.columns)}")
        logger.info(
            f"💾 PROCESS_SECTOR_UPLOAD: Memory usage: {df.memory_usage(deep=True).sum() / (1024 * 1024):.2f} MB"
        )

        # Convert target path to .parquet extension
        base_name = Path(target_path).stem
        parquet_path = str(Path(target_path).parent / f"{base_name}.parquet")
        logger.info(f"🔄 PROCESS_SECTOR_UPLOAD: Converting path to Parquet format")
        logger.info(f"📁 PROCESS_SECTOR_UPLOAD: Original target: {target_path}")
        logger.info(f"📁 PROCESS_SECTOR_UPLOAD: Parquet target: {parquet_path}")

        # Save as Parquet for internal processing
        logger.info(f"💾 PROCESS_SECTOR_UPLOAD: Saving sector DataFrame as Parquet")
        save_parquet(df, parquet_path)

        logger.info(
            f"✅ PROCESS_SECTOR_UPLOAD: Successfully converted sector file to Parquet"
        )
        logger.info(f"📍 PROCESS_SECTOR_UPLOAD: Final location: {parquet_path}")
        logger.info(f"📊 PROCESS_SECTOR_UPLOAD: Final DataFrame shape: {df.shape}")

        return df, parquet_path

    except Exception as e:
        logger.error(
            f"❌ PROCESS_SECTOR_UPLOAD: Failed to process sector file {uploaded_file.name}"
        )
        logger.error(f"💥 PROCESS_SECTOR_UPLOAD: Error details: {str(e)}")
        logger.error(f"🔍 PROCESS_SECTOR_UPLOAD: Error type: {type(e).__name__}")
        if "sheet_name" in str(e).lower():
            logger.error(
                f"📋 PROCESS_SECTOR_UPLOAD: Sheet name issue - Expected: '{expected_sheet_name}'"
            )
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
    logger.info(f"🚀 VALIDATE_AND_PROCESS_FILE: Starting unified file processing")
    logger.info(f"📂 VALIDATE_AND_PROCESS_FILE: File type: {file_type}")
    logger.info(f"📋 VALIDATE_AND_PROCESS_FILE: Expected sheet: {expected_sheet}")
    logger.info(f"📁 VALIDATE_AND_PROCESS_FILE: Target path: {target_path}")

    try:
        file_name = uploaded_file.name if hasattr(uploaded_file, "name") else "Unknown"
        logger.info(f"📤 VALIDATE_AND_PROCESS_FILE: Processing file: {file_name}")

        if file_type.lower() == "sfw":
            logger.info(f"🏢 VALIDATE_AND_PROCESS_FILE: Routing to SFW processor")
            result = process_sfw_upload(uploaded_file, expected_sheet, target_path)
        elif file_type.lower() == "sector":
            logger.info(f"🏭 VALIDATE_AND_PROCESS_FILE: Routing to sector processor")
            result = process_sector_upload(uploaded_file, expected_sheet, target_path)
        else:
            logger.info(
                f"📄 VALIDATE_AND_PROCESS_FILE: Routing to generic file processor"
            )
            result = process_uploaded_file(uploaded_file, target_path)

        logger.info(
            f"✅ VALIDATE_AND_PROCESS_FILE: File processing completed successfully"
        )
        logger.info(
            f"📊 VALIDATE_AND_PROCESS_FILE: Result DataFrame shape: {result[0].shape}"
        )
        logger.info(f"📁 VALIDATE_AND_PROCESS_FILE: Result path: {result[1]}")

        return result

    except Exception as e:
        logger.error(f"❌ VALIDATE_AND_PROCESS_FILE: Failed to process file")
        logger.error(f"💥 VALIDATE_AND_PROCESS_FILE: Error details: {str(e)}")
        logger.error(f"🔍 VALIDATE_AND_PROCESS_FILE: Error type: {type(e).__name__}")
        raise


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
    logger.info(f"📊 GET_FILE_INFO_SUMMARY: Generating file summary")
    logger.info(f"📄 GET_FILE_INFO_SUMMARY: Original filename: {original_name}")
    logger.info(f"📁 GET_FILE_INFO_SUMMARY: Parquet path: {parquet_path}")

    try:
        # Calculate memory usage
        memory_usage_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)

        # Get data types
        data_types = df.dtypes.to_dict()

        # Create summary
        summary = {
            "original_filename": original_name,
            "parquet_path": parquet_path,
            "shape": df.shape,
            "columns": list(df.columns),
            "memory_usage_mb": memory_usage_mb,
            "data_types": data_types,
        }

        logger.info(f"📈 GET_FILE_INFO_SUMMARY: DataFrame shape: {df.shape}")
        logger.info(f"📋 GET_FILE_INFO_SUMMARY: Number of columns: {len(df.columns)}")
        logger.info(f"💾 GET_FILE_INFO_SUMMARY: Memory usage: {memory_usage_mb:.2f} MB")
        logger.info(
            f"📊 GET_FILE_INFO_SUMMARY: Data types: {len(set(data_types.values()))} unique types"
        )
        logger.info(f"✅ GET_FILE_INFO_SUMMARY: Summary generated successfully")

        return summary

    except Exception as e:
        logger.error(f"❌ GET_FILE_INFO_SUMMARY: Failed to generate file summary")
        logger.error(f"💥 GET_FILE_INFO_SUMMARY: Error details: {str(e)}")
        logger.error(f"🔍 GET_FILE_INFO_SUMMARY: Error type: {type(e).__name__}")
        raise
