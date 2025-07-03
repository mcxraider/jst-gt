# utils/format_converter.py
"""
Utility to convert existing CSV and Excel files to Parquet format.
This helps migrate existing data to the high-performance Parquet format.
"""
import pandas as pd
import logging
from pathlib import Path
from typing import List, Optional
import os

from services.storage import save_parquet, load_csv, load_excel
from services.storage.file_management import list_files
from config import (
    USE_S3,
    INPUT_DATA_PATH,
    OUTPUT_PATH,
    INTERMEDIATE_OUTPUT_PATH,
    MISC_OUTPUT_PATH,
)

logger = logging.getLogger(__name__)


def convert_excel_to_parquet(
    excel_path: str, output_path: Optional[str] = None, sheet_name: Optional[str] = None
) -> str:
    """
    Convert Excel file to Parquet format.

    Args:
        excel_path (str): Path to Excel file
        output_path (str, optional): Output path for Parquet file. If None, replaces extension.
        sheet_name (str, optional): Specific sheet to convert. If None, converts first sheet.

    Returns:
        str: Path to created Parquet file
    """
    try:
        logger.info(f"🔄 Converting Excel to Parquet: {excel_path}")

        # Load Excel file
        if sheet_name:
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
        else:
            df = pd.read_excel(excel_path)

        # Determine output path
        if output_path is None:
            base_path = Path(excel_path)
            output_path = str(base_path.with_suffix(".parquet"))

        # Save as Parquet
        save_parquet(df, output_path)
        logger.info(f"✅ Converted Excel to Parquet: {output_path} (Shape: {df.shape})")
        return output_path

    except Exception as e:
        logger.error(f"❌ Failed to convert Excel to Parquet: {e}")
        raise


def convert_csv_to_parquet(csv_path: str, output_path: Optional[str] = None) -> str:
    """
    Convert CSV file to Parquet format.

    Args:
        csv_path (str): Path to CSV file
        output_path (str, optional): Output path for Parquet file. If None, replaces extension.

    Returns:
        str: Path to created Parquet file
    """
    try:
        logger.info(f"🔄 Converting CSV to Parquet: {csv_path}")

        # Load CSV file
        df = load_csv(csv_path)

        # Determine output path
        if output_path is None:
            base_path = Path(csv_path)
            output_path = str(base_path.with_suffix(".parquet"))

        # Save as Parquet
        save_parquet(df, output_path)
        logger.info(f"✅ Converted CSV to Parquet: {output_path} (Shape: {df.shape})")
        return output_path

    except Exception as e:
        logger.error(f"❌ Failed to convert CSV to Parquet: {e}")
        raise


def batch_convert_directory(
    directory: str, file_pattern: str, target_format: str = "parquet"
) -> List[str]:
    """
    Convert all files matching a pattern in a directory to Parquet format.

    Args:
        directory (str): Directory path to search
        file_pattern (str): File pattern to match (e.g., "*.csv", "*.xlsx")
        target_format (str): Target format (currently only "parquet" supported)

    Returns:
        List[str]: List of created Parquet file paths
    """
    if target_format != "parquet":
        raise ValueError("Only 'parquet' target format is currently supported")

    try:
        logger.info(
            f"🔄 Batch converting files in {directory} with pattern {file_pattern}"
        )

        # List files matching pattern
        files = list_files(directory, file_pattern)
        converted_files = []

        for file_path in files:
            try:
                file_ext = Path(file_path).suffix.lower()

                if file_ext in [".xlsx", ".xls"]:
                    converted_path = convert_excel_to_parquet(file_path)
                elif file_ext == ".csv":
                    converted_path = convert_csv_to_parquet(file_path)
                else:
                    logger.warning(f"⚠️ Unsupported file format: {file_path}")
                    continue

                converted_files.append(converted_path)

            except Exception as e:
                logger.error(f"❌ Failed to convert {file_path}: {e}")
                continue

        logger.info(
            f"✅ Batch conversion completed. Converted {len(converted_files)} files"
        )
        return converted_files

    except Exception as e:
        logger.error(f"❌ Batch conversion failed: {e}")
        raise


def migrate_workspace_to_parquet(backup_originals: bool = True) -> dict:
    """
    Migrate entire workspace from CSV/Excel to Parquet format.

    Args:
        backup_originals (bool): Whether to backup original files before conversion

    Returns:
        dict: Summary of conversion results
    """
    results = {
        "input_files": [],
        "output_files": [],
        "intermediate_files": [],
        "misc_files": [],
        "errors": [],
    }

    directories_to_convert = [
        (INPUT_DATA_PATH, "input_files"),
        (OUTPUT_PATH, "output_files"),
        (INTERMEDIATE_OUTPUT_PATH, "intermediate_files"),
        (MISC_OUTPUT_PATH, "misc_files"),
    ]

    logger.info("🚀 Starting workspace migration to Parquet format")

    for directory, result_key in directories_to_convert:
        try:
            # Convert Excel files
            excel_files = batch_convert_directory(directory, "*.xlsx")
            results[result_key].extend(excel_files)

            # Convert CSV files
            csv_files = batch_convert_directory(directory, "*.csv")
            results[result_key].extend(csv_files)

        except Exception as e:
            error_msg = f"Failed to convert files in {directory}: {e}"
            logger.error(f"❌ {error_msg}")
            results["errors"].append(error_msg)

    # Summary
    total_converted = sum(len(results[key]) for key in results if key != "errors")
    logger.info(
        f"✅ Workspace migration completed. Total files converted: {total_converted}"
    )

    if results["errors"]:
        logger.warning(f"⚠️ {len(results['errors'])} errors occurred during migration")

    return results


def get_conversion_summary(directory: str) -> dict:
    """
    Get a summary of files that can be converted to Parquet in a directory.

    Args:
        directory (str): Directory to analyze

    Returns:
        dict: Summary including file counts and estimated space savings
    """
    try:
        excel_files = list_files(directory, "*.xlsx")
        csv_files = list_files(directory, "*.csv")
        parquet_files = list_files(directory, "*.parquet")

        return {
            "excel_files": len(excel_files),
            "csv_files": len(csv_files),
            "parquet_files": len(parquet_files),
            "total_convertible": len(excel_files) + len(csv_files),
            "excel_paths": excel_files,
            "csv_paths": csv_files,
            "parquet_paths": parquet_files,
        }

    except Exception as e:
        logger.error(f"❌ Failed to analyze directory {directory}: {e}")
        return {"error": str(e)}
