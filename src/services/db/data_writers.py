# services/db/data_writers.py
"""
Data writing functions for saving processed results to various output directories.
Handles input files, output files, and intermediate processing results.
"""
import os
import pandas as pd
import asyncio
import logging
from pathlib import Path

from config import (
    INPUT_DATA_PATH,
    OUTPUT_PATH,
    INTERMEDIATE_OUTPUT_PATH,
    MISC_OUTPUT_PATH,
)
from services.storage import save_parquet
from .file_utils import rename_input_file, rename_output_file

# Configure logging
logger = logging.getLogger(__name__)


def write_input_file(abs_path, df, file_name):
    """
    Write DataFrame to input directory in Parquet format.

    Args:
        abs_path (str): Absolute path to directory or S3 URI
        df (pd.DataFrame): DataFrame to save
        file_name (str): File name with extension (will be converted to .parquet)

    Note:
        All input files are now saved in Parquet format for optimal performance.
        The extension will be changed to .parquet regardless of the original extension.
    """
    logger.info(f"📝 WRITE_INPUT_FILE: Starting write operation")
    logger.info(f"📂 WRITE_INPUT_FILE: Target directory: {abs_path}")
    logger.info(f"📄 WRITE_INPUT_FILE: Original filename: {file_name}")
    logger.info(f"📊 WRITE_INPUT_FILE: DataFrame shape: {df.shape}")

    # Convert file extension to .parquet
    base_name = Path(file_name).stem
    parquet_name = f"{base_name}.parquet"
    logger.info(f"🔄 WRITE_INPUT_FILE: Converted filename: {parquet_name}")

    if abs_path.startswith("s3://"):
        logger.info("☁️ WRITE_INPUT_FILE: Using S3 storage mode")
        path = abs_path.rstrip("/") + "/" + parquet_name.lstrip("/")
        logger.info(f"🔗 WRITE_INPUT_FILE: S3 path constructed: {path}")
    else:
        logger.info("💾 WRITE_INPUT_FILE: Using local filesystem mode")
        path = Path(abs_path) / parquet_name
        logger.info(f"📁 WRITE_INPUT_FILE: Local path constructed: {path}")

    logger.info(f"🎯 WRITE_INPUT_FILE: Final path: {str(path)}")

    try:
        save_parquet(df, str(path))
        logger.info(f"✅ WRITE_INPUT_FILE: Successfully saved file to {str(path)}")
    except Exception as e:
        logger.error(f"❌ WRITE_INPUT_FILE: Failed to save file: {e}")
        raise


def write_output_file(abs_path: str, df: pd.DataFrame, file_name: str):
    """
    Write DataFrame as Parquet to output directory.

    Args:
        abs_path (str): Absolute path to directory or S3 URI
        df (pd.DataFrame): DataFrame to save
        file_name (str): File name (without extension, .parquet will be added)
    """
    logger.info(f"📝 WRITE_OUTPUT_FILE: Starting write operation")
    logger.info(f"📂 WRITE_OUTPUT_FILE: Target directory: {abs_path}")
    logger.info(f"📄 WRITE_OUTPUT_FILE: Filename (without extension): {file_name}")
    logger.info(f"📊 WRITE_OUTPUT_FILE: DataFrame shape: {df.shape}")

    if abs_path.startswith("s3://"):
        logger.info("☁️ WRITE_OUTPUT_FILE: Using S3 storage mode")
        # For S3, concatenate as string
        path = abs_path.rstrip("/") + "/" + file_name.lstrip("/") + ".parquet"
        logger.info(f"🔗 WRITE_OUTPUT_FILE: S3 path constructed: {path}")
    else:
        logger.info("💾 WRITE_OUTPUT_FILE: Using local filesystem mode")
        # For local, use Path
        path = Path(abs_path) / f"{file_name}.parquet"
        logger.info(f"📁 WRITE_OUTPUT_FILE: Local path constructed: {path}")

    logger.info(f"🎯 WRITE_OUTPUT_FILE: Final path: {str(path)}")

    try:
        save_parquet(df, str(path))
        logger.info(f"✅ WRITE_OUTPUT_FILE: Successfully saved file to {str(path)}")
    except Exception as e:
        logger.error(f"❌ WRITE_OUTPUT_FILE: Failed to save file: {e}")
        raise


async def write_input_to_s3(
    sfw_filename: str,
    sfw_df: pd.DataFrame,
    sector_filename: str,
    sector_df: pd.DataFrame,
):
    """
    Asynchronously write input files (SFW and sector) to storage with timestamps.

    Args:
        sfw_filename (str): SFW file name
        sfw_df (pd.DataFrame): SFW DataFrame
        sector_filename (str): Sector file name
        sector_df (pd.DataFrame): Sector DataFrame

    Note:
        Creates input directory if it doesn't exist.
        Renames files with timestamps before saving.
    """
    logger.info(f"🚀 WRITE_INPUT_TO_S3: Starting async input file write operation")
    logger.info(f"📂 WRITE_INPUT_TO_S3: Target path: {INPUT_DATA_PATH}")
    logger.info(f"📄 WRITE_INPUT_TO_S3: SFW filename: {sfw_filename}")
    logger.info(f"📄 WRITE_INPUT_TO_S3: Sector filename: {sector_filename}")
    logger.info(f"📊 WRITE_INPUT_TO_S3: SFW DataFrame shape: {sfw_df.shape}")
    logger.info(f"📊 WRITE_INPUT_TO_S3: Sector DataFrame shape: {sector_df.shape}")

    abs_path = INPUT_DATA_PATH

    # Only create directories for local paths, not S3
    if not abs_path.startswith("s3://"):
        logger.info("📁 WRITE_INPUT_TO_S3: Creating local directories")
        os.makedirs(abs_path, exist_ok=True)
    else:
        logger.info(
            "☁️ WRITE_INPUT_TO_S3: Using S3 storage, no local directory creation needed"
        )

    # Generate timestamped file names
    logger.info("⏰ WRITE_INPUT_TO_S3: Generating timestamped filenames")
    renamed_sfw, renamed_sector = await asyncio.gather(
        rename_input_file(sfw_filename),
        rename_input_file(sector_filename),
    )
    logger.info(f"🔄 WRITE_INPUT_TO_S3: Renamed SFW file: {renamed_sfw}")
    logger.info(f"🔄 WRITE_INPUT_TO_S3: Renamed sector file: {renamed_sector}")

    # Write files concurrently
    logger.info("📝 WRITE_INPUT_TO_S3: Starting concurrent file writes")
    loop = asyncio.get_running_loop()

    try:
        await asyncio.gather(
            loop.run_in_executor(None, write_input_file, abs_path, sfw_df, renamed_sfw),
            loop.run_in_executor(
                None, write_input_file, abs_path, sector_df, renamed_sector
            ),
        )
        logger.info("✅ WRITE_INPUT_TO_S3: All input files written successfully")
    except Exception as e:
        logger.error(f"❌ WRITE_INPUT_TO_S3: Failed to write input files: {e}")
        raise


async def write_output_to_s3(dfs: list[tuple[pd.DataFrame, str]]):
    """
    Asynchronously write multiple output DataFrames to storage.

    Args:
        dfs (list): List of tuples (DataFrame, filename)

    Raises:
        ValueError: If any item in dfs is not a proper tuple

    Note:
        Creates output directory if it doesn't exist.
        Renames files with '_output' suffix before saving.
    """
    logger.info(f"🚀 WRITE_OUTPUT_TO_S3: Starting async output file write operation")
    logger.info(f"📂 WRITE_OUTPUT_TO_S3: Target path: {OUTPUT_PATH}")
    logger.info(f"📊 WRITE_OUTPUT_TO_S3: Number of DataFrames to write: {len(dfs)}")

    abs_path = OUTPUT_PATH

    # Only create directories for local paths, not S3
    if not abs_path.startswith("s3://"):
        logger.info("📁 WRITE_OUTPUT_TO_S3: Creating local directories")
        os.makedirs(abs_path, exist_ok=True)
    else:
        logger.info(
            "☁️ WRITE_OUTPUT_TO_S3: Using S3 storage, no local directory creation needed"
        )

    # Validate input format
    logger.info("🔍 WRITE_OUTPUT_TO_S3: Validating input format")
    for i, item in enumerate(dfs):
        if not isinstance(item, tuple) or len(item) != 2:
            error_msg = f"dfs[{i}] must be a tuple (DataFrame, str), but got: {item}"
            logger.error(f"❌ WRITE_OUTPUT_TO_S3: {error_msg}")
            raise ValueError(f"[ERROR] {error_msg}")

        df, filename = item
        logger.info(
            f"📋 WRITE_OUTPUT_TO_S3: Item {i}: filename='{filename}', shape={df.shape}"
        )

    # Generate output file names
    logger.info("⏰ WRITE_OUTPUT_TO_S3: Generating output filenames")
    rename_tasks = [rename_output_file(fname) for _, fname in dfs]
    new_names = await asyncio.gather(*rename_tasks)

    for i, (old_name, new_name) in enumerate(
        zip([fname for _, fname in dfs], new_names)
    ):
        logger.info(
            f"🔄 WRITE_OUTPUT_TO_S3: Renamed file {i}: '{old_name}' -> '{new_name}'"
        )

    # Write files concurrently
    logger.info("📝 WRITE_OUTPUT_TO_S3: Starting concurrent file writes")
    loop = asyncio.get_running_loop()
    write_tasks = [
        loop.run_in_executor(None, write_output_file, abs_path, df, new_name)
        for (df, _), new_name in zip(dfs, new_names)
    ]

    try:
        await asyncio.gather(*write_tasks)
        logger.info("✅ WRITE_OUTPUT_TO_S3: All output files written successfully")
    except Exception as e:
        logger.error(f"❌ WRITE_OUTPUT_TO_S3: Failed to write output files: {e}")
        raise


def write_r1_invalid_to_s3(df: pd.DataFrame, target_sector_alias: str):
    """
    Write Round 1 invalid skills data to intermediate output directory.

    Args:
        df (pd.DataFrame): Invalid skills DataFrame
        target_sector_alias (str): Sector alias for file naming
    """
    logger.info(f"📝 WRITE_R1_INVALID_TO_S3: Starting R1 invalid skills write")
    logger.info(
        f"🏷️ WRITE_R1_INVALID_TO_S3: Target sector alias: {target_sector_alias}"
    )
    logger.info(f"📊 WRITE_R1_INVALID_TO_S3: DataFrame shape: {df.shape}")

    path = (
        f"{INTERMEDIATE_OUTPUT_PATH}/{target_sector_alias}_r1_invalid_skill_pl.parquet"
    )
    logger.info(f"🎯 WRITE_R1_INVALID_TO_S3: Target path: {path}")

    try:
        save_parquet(df, path)
        logger.info(
            f"✅ WRITE_R1_INVALID_TO_S3: Successfully saved R1 invalid skills data"
        )
    except Exception as e:
        logger.error(
            f"❌ WRITE_R1_INVALID_TO_S3: Failed to save R1 invalid skills data: {e}"
        )
        raise


def write_r1_valid_to_s3(df: pd.DataFrame, target_sector_alias: str):
    """
    Write Round 1 valid skills data to intermediate output directory.

    Args:
        df (pd.DataFrame): Valid skills DataFrame
        target_sector_alias (str): Sector alias for file naming
    """
    logger.info(f"📝 WRITE_R1_VALID_TO_S3: Starting R1 valid skills write")
    logger.info(f"🏷️ WRITE_R1_VALID_TO_S3: Target sector alias: {target_sector_alias}")
    logger.info(f"📊 WRITE_R1_VALID_TO_S3: DataFrame shape: {df.shape}")

    path = f"{INTERMEDIATE_OUTPUT_PATH}/{target_sector_alias}_r1_valid_skill_pl.parquet"
    logger.info(f"🎯 WRITE_R1_VALID_TO_S3: Target path: {path}")

    try:
        save_parquet(df, path)
        logger.info(f"✅ WRITE_R1_VALID_TO_S3: Successfully saved R1 valid skills data")
    except Exception as e:
        logger.error(
            f"❌ WRITE_R1_VALID_TO_S3: Failed to save R1 valid skills data: {e}"
        )
        raise


def write_irrelevant_to_s3(df: pd.DataFrame, target_sector_alias: str):
    """
    Write irrelevant skills data to intermediate output directory.

    Args:
        df (pd.DataFrame): Irrelevant skills DataFrame
        target_sector_alias (str): Sector alias for file naming
    """
    logger.info(f"📝 WRITE_IRRELEVANT_TO_S3: Starting irrelevant skills write")
    logger.info(
        f"🏷️ WRITE_IRRELEVANT_TO_S3: Target sector alias: {target_sector_alias}"
    )
    logger.info(f"📊 WRITE_IRRELEVANT_TO_S3: DataFrame shape: {df.shape}")

    path = f"{INTERMEDIATE_OUTPUT_PATH}/{target_sector_alias}_r1_irrelevant.parquet"
    logger.info(f"🎯 WRITE_IRRELEVANT_TO_S3: Target path: {path}")

    try:
        save_parquet(df, path)
        logger.info(
            f"✅ WRITE_IRRELEVANT_TO_S3: Successfully saved irrelevant skills data"
        )
    except Exception as e:
        logger.error(
            f"❌ WRITE_IRRELEVANT_TO_S3: Failed to save irrelevant skills data: {e}"
        )
        raise


def write_r2_raw_to_s3(df: pd.DataFrame, target_sector_alias: str):
    """
    Write Round 2 raw course skill data to miscellaneous output directory.

    Args:
        df (pd.DataFrame): Raw course skill DataFrame
        target_sector_alias (str): Sector alias for file naming
    """
    logger.info(f"📝 WRITE_R2_RAW_TO_S3: Starting R2 raw course skill write")
    logger.info(f"🏷️ WRITE_R2_RAW_TO_S3: Target sector alias: {target_sector_alias}")
    logger.info(f"📊 WRITE_R2_RAW_TO_S3: DataFrame shape: {df.shape}")

    path = f"{MISC_OUTPUT_PATH}/{target_sector_alias}_course_skill_pl_rac_raw.parquet"
    logger.info(f"🎯 WRITE_R2_RAW_TO_S3: Target path: {path}")

    try:
        save_parquet(df, path)
        logger.info(
            f"✅ WRITE_R2_RAW_TO_S3: Successfully saved R2 raw course skill data"
        )
    except Exception as e:
        logger.error(
            f"❌ WRITE_R2_RAW_TO_S3: Failed to save R2 raw course skill data: {e}"
        )
        raise


def write_missing_to_s3(df: pd.DataFrame, target_sector_alias: str):
    """
    Write courses with missing content to miscellaneous output directory.

    Args:
        df (pd.DataFrame): DataFrame containing courses with missing content.
        target_sector_alias (str): Sector alias used for naming the output file.
    """
    logger.info(f"📝 WRITE_MISSING_TO_S3: Starting missing content courses write")
    logger.info(f"🏷️ WRITE_MISSING_TO_S3: Target sector alias: {target_sector_alias}")
    logger.info(f"📊 WRITE_MISSING_TO_S3: DataFrame shape: {df.shape}")

    path = f"{MISC_OUTPUT_PATH}/{target_sector_alias}_missing_content_course.parquet"
    logger.info(f"🎯 WRITE_MISSING_TO_S3: Target path: {path}")

    try:
        save_parquet(df, path)
        logger.info(
            f"✅ WRITE_MISSING_TO_S3: Successfully saved missing content courses data"
        )
    except Exception as e:
        logger.error(
            f"❌ WRITE_MISSING_TO_S3: Failed to save missing content courses data: {e}"
        )
        raise


def write_rest_to_s3(df: pd.DataFrame, target_sector_alias: str):
    """
    Write courses with poor content quality to miscellaneous output directory.

    Args:
        df (pd.DataFrame): DataFrame containing courses with poor content quality.
        target_sector_alias (str): Sector alias used for naming the output file.
    """
    logger.info(f"📝 WRITE_REST_TO_S3: Starting poor content quality courses write")
    logger.info(f"🏷️ WRITE_REST_TO_S3: Target sector alias: {target_sector_alias}")
    logger.info(f"📊 WRITE_REST_TO_S3: DataFrame shape: {df.shape}")

    path = (
        f"{MISC_OUTPUT_PATH}/{target_sector_alias}_poor_content_quality_course.parquet"
    )
    logger.info(f"🎯 WRITE_REST_TO_S3: Target path: {path}")

    try:
        save_parquet(df, path)
        logger.info(
            f"✅ WRITE_REST_TO_S3: Successfully saved poor content quality courses data"
        )
    except Exception as e:
        logger.error(
            f"❌ WRITE_REST_TO_S3: Failed to save poor content quality courses data: {e}"
        )
        raise
