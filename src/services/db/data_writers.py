# services/db/data_writers.py
"""
Data writing functions for saving processed results to various output directories.
Handles input files, output files, and intermediate processing results.

This module provides a comprehensive set of functions for writing data to
various storage locations. It supports both synchronous and asynchronous
writing operations, with specific functions for different data types and
processing stages. The module handles file naming, directory creation,
and format-specific saving operations.
"""
import os
import pandas as pd
import asyncio
from pathlib import Path

from config import (
    INPUT_DATA_PATH,
    OUTPUT_PATH,
    INTERMEDIATE_OUTPUT_PATH,
    MISC_OUTPUT_PATH,
)
from services.storage import save_csv, save_excel
from .file_utils import rename_input_file, rename_output_file


def write_input_file(abs_path, df, file_name):
    """
    Write DataFrame to input directory in appropriate format based on extension.
    
    Saves the DataFrame to the specified path using the appropriate format
    based on the file extension (.csv or .xlsx).
    
    Args:
        abs_path (str): Absolute path to directory
        df (pd.DataFrame): DataFrame to save
        file_name (str): File name with extension
        
    Raises:
        ValueError: If file extension is not supported (.csv or .xlsx)
    """
    path = Path(abs_path) / file_name
    ext = path.suffix.lower()
    if ext == ".csv":
        save_csv(df, str(path))
    elif ext == ".xlsx":
        save_excel(df, str(path))
    else:
        raise ValueError(f"Unsupported extension: '{ext}'")


def write_output_file(abs_path: str, df: pd.DataFrame, file_name: str):
    """
    Write DataFrame as CSV to output directory.
    
    Saves the DataFrame as a CSV file in the specified output directory.
    Automatically adds .csv extension if not present.
    
    Args:
        abs_path (str): Absolute path to directory
        df (pd.DataFrame): DataFrame to save
        file_name (str): File name (without extension, .csv will be added)
    """
    path = Path(abs_path) / f"{file_name}.csv"
    save_csv(df, str(path))


async def write_input_to_s3(
    sfw_filename: str,
    sfw_df: pd.DataFrame,
    sector_filename: str,
    sector_df: pd.DataFrame,
):
    """
    Asynchronously write input files (SFW and sector) to storage with timestamps.
    
    Concurrently saves both SFW and sector input files with timestamped names.
    Creates the input directory if it doesn't exist.
    
    Args:
        sfw_filename (str): SFW file name
        sfw_df (pd.DataFrame): SFW DataFrame
        sector_filename (str): Sector file name
        sector_df (pd.DataFrame): Sector DataFrame
        
    Note:
        - Creates input directory if it doesn't exist
        - Renames files with timestamps before saving
        - Uses concurrent execution for better performance
    """
    abs_path = INPUT_DATA_PATH
    os.makedirs(abs_path, exist_ok=True)

    # Generate timestamped file names
    renamed_sfw, renamed_sector = await asyncio.gather(
        rename_input_file(sfw_filename),
        rename_input_file(sector_filename),
    )

    # Write files concurrently
    loop = asyncio.get_running_loop()
    await asyncio.gather(
        loop.run_in_executor(None, write_input_file, abs_path, sfw_df, renamed_sfw),
        loop.run_in_executor(
            None, write_input_file, abs_path, sector_df, renamed_sector
        ),
    )


async def write_output_to_s3(dfs: list[tuple[pd.DataFrame, str]]):
    """
    Asynchronously write multiple output DataFrames to storage.
    
    Concurrently saves multiple DataFrames to the output directory with
    standardized naming conventions.
    
    Args:
        dfs (list): List of tuples (DataFrame, filename)
        
    Raises:
        ValueError: If any item in dfs is not a proper tuple
        
    Note:
        - Creates output directory if it doesn't exist
        - Renames files with '_output' suffix before saving
        - Uses concurrent execution for better performance
    """
    abs_path = OUTPUT_PATH
    os.makedirs(abs_path, exist_ok=True)

    # Validate input format
    for i, item in enumerate(dfs):
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError(
                f"[ERROR] dfs[{i}] must be a tuple (DataFrame, str), but got: {item}"
            )

    # Generate output file names
    rename_tasks = [rename_output_file(fname) for _, fname in dfs]
    new_names = await asyncio.gather(*rename_tasks)

    # Write files concurrently
    loop = asyncio.get_running_loop()
    write_tasks = [
        loop.run_in_executor(None, write_output_file, abs_path, df, new_name)
        for (df, _), new_name in zip(dfs, new_names)
    ]
    await asyncio.gather(*write_tasks)


def write_r1_invalid_to_s3(df: pd.DataFrame, target_sector_alias: str):
    """
    Write Round 1 invalid skills data to intermediate output directory.
    
    Saves the DataFrame containing skills that were marked as invalid
    during Round 1 processing.
    
    Args:
        df (pd.DataFrame): Invalid skills DataFrame
        target_sector_alias (str): Sector alias for file naming
    """
    path = f"{INTERMEDIATE_OUTPUT_PATH}/{target_sector_alias}_r1_invalid_skill_pl.csv"
    save_csv(df, path)


def write_r1_valid_to_s3(df: pd.DataFrame, target_sector_alias: str):
    """
    Write Round 1 valid skills data to intermediate output directory.
    
    Saves the DataFrame containing skills that were successfully validated
    during Round 1 processing.
    
    Args:
        df (pd.DataFrame): Valid skills DataFrame
        target_sector_alias (str): Sector alias for file naming
    """
    path = f"{INTERMEDIATE_OUTPUT_PATH}/{target_sector_alias}_r1_valid_skill_pl.csv"
    save_csv(df, path)


def write_irrelevant_to_s3(df: pd.DataFrame, target_sector_alias: str):
    """
    Write irrelevant skills data to intermediate output directory.
    
    Saves the DataFrame containing skills that were determined to be
    irrelevant to the target sector.
    
    Args:
        df (pd.DataFrame): Irrelevant skills DataFrame
        target_sector_alias (str): Sector alias for file naming
    """
    path = f"{INTERMEDIATE_OUTPUT_PATH}/{target_sector_alias}_r1_irrelevant.csv"
    save_csv(df, path)


def write_r2_raw_to_s3(df: pd.DataFrame, target_sector_alias: str):
    """
    Write Round 2 raw course skill data to miscellaneous output directory.
    
    Saves the raw course skill data that will be processed in Round 2
    to the miscellaneous output directory.
    
    Args:
        df (pd.DataFrame): Raw course skill DataFrame
        target_sector_alias (str): Sector alias for file naming
    """
    path = f"{MISC_OUTPUT_PATH}/{target_sector_alias}_course_skill_pl_rac_raw.csv"
    save_csv(df, path)


def write_missing_to_s3(df: pd.DataFrame, target_sector_alias: str):
    """
    Write courses with missing content to miscellaneous output directory.
    
    Saves the DataFrame containing courses that were identified as having
    missing or incomplete content.
    
    Args:
        df (pd.DataFrame): DataFrame containing courses with missing content
        target_sector_alias (str): Sector alias used for naming the output file
    """
    path = f"{MISC_OUTPUT_PATH}/{target_sector_alias}_missing_content_course.csv"
    save_csv(df, path)


def write_rest_to_s3(df: pd.DataFrame, target_sector_alias: str):
    """
    Write courses with poor content quality to miscellaneous output directory.
    
    Saves the DataFrame containing courses that were identified as having
    poor content quality or insufficient information.
    
    Args:
        df (pd.DataFrame): DataFrame containing courses with poor content quality
        target_sector_alias (str): Sector alias used for naming the output file
    """
    path = f"{MISC_OUTPUT_PATH}/{target_sector_alias}_poor_content_quality_course.csv"
    save_csv(df, path)
