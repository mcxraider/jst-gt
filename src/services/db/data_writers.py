# services/db/data_writers.py
"""
Data writing functions for saving processed results to various output directories.
Handles input files, output files, and intermediate processing results.
"""
import os
import pandas as pd
import asyncio
from pathlib import Path

from config import (
    input_data_path,
    output_path,
    intermediate_output_path,
    misc_output_path,
)
from services.storage import save_csv, save_excel
from .file_utils import rename_input_file, rename_output_file


def write_input_file(abs_path, df, file_name):
    """
    Write DataFrame to input directory in appropriate format based on extension.

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

    Args:
        sfw_filename (str): SFW file name
        sfw_df (pd.DataFrame): SFW DataFrame
        sector_filename (str): Sector file name
        sector_df (pd.DataFrame): Sector DataFrame

    Note:
        Creates input directory if it doesn't exist.
        Renames files with timestamps before saving.
    """
    abs_path = input_data_path
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

    Args:
        dfs (list): List of tuples (DataFrame, filename)

    Raises:
        ValueError: If any item in dfs is not a proper tuple

    Note:
        Creates output directory if it doesn't exist.
        Renames files with '_output' suffix before saving.
    """
    abs_path = output_path
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

    Args:
        df (pd.DataFrame): Invalid skills DataFrame
        target_sector_alias (str): Sector alias for file naming
    """
    path = f"{intermediate_output_path}/{target_sector_alias}_r1_invalid_skill_pl.csv"
    save_csv(df, path)
    print(f"Saved invalid R1 output to {path}")


def write_r1_valid_to_s3(df: pd.DataFrame, target_sector_alias: str):
    """
    Write Round 1 valid skills data to intermediate output directory.

    Args:
        df (pd.DataFrame): Valid skills DataFrame
        target_sector_alias (str): Sector alias for file naming
    """
    path = f"{intermediate_output_path}/{target_sector_alias}_r1_valid_skill_pl.csv"
    save_csv(df, path)
    print(f"Saved valid R1 output to {path}")


def write_irrelevant_to_s3(df: pd.DataFrame, target_sector_alias: str):
    """
    Write irrelevant skills data to intermediate output directory.

    Args:
        df (pd.DataFrame): Irrelevant skills DataFrame
        target_sector_alias (str): Sector alias for file naming
    """
    path = f"{intermediate_output_path}/{target_sector_alias}_r1_irrelevant.csv"
    save_csv(df, path)
    print(f"Saved irrelevant output to {path}")


def write_r2_raw_to_s3(df: pd.DataFrame, target_sector_alias: str):
    """
    Write Round 2 raw course skill data to miscellaneous output directory.

    Args:
        df (pd.DataFrame): Raw course skill DataFrame
        target_sector_alias (str): Sector alias for file naming
    """
    path = f"{misc_output_path}/{target_sector_alias}_course_skill_pl_rac_raw.csv"
    save_csv(df, path)
    print(f"Saved R2 raw output to {path}")
