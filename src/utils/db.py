import streamlit as st
import pandas as pd
import os
import asyncio
from typing import Optional, Tuple, List, Any, Callable
from pathlib import Path


from utils.input_handler import rename_input_file
from utils.output_handler import rename_output_file


def _fetch_df(path: str) -> tuple[pd.DataFrame, str]:
    """
    Helper to load a CSV and return the DataFrame along with its base filename (without extension).
    """
    filepath = Path(path)
    df = pd.read_csv(filepath)
    name = filepath.stem
    return df, name


def fetch_valid(path: str = "../temp_output/PublicTransport_all_valid_skill_pl_date.csv") -> tuple[pd.DataFrame, str]:
    """
    Fetch the 'valid' skills CSV.
    """
    return _fetch_df(path)


def fetch_irrelevant(path: str = "../temp_output/PubTransport_irrelevant_skills.csv") -> tuple[pd.DataFrame, str]:
    """
    Fetch the 'irrelevant' skills CSV.
    """
    return _fetch_df(path)


def fetch_invalid(path: str = "../temp_output/PubTransport_r2_invalid_skll_pl_date.csv") -> tuple[pd.DataFrame, str]:
    """
    Fetch the 'invalid' skills CSV.
    """
    return _fetch_df(path)


def fetch_completed_output():
    # generate final DataFrames
    valid = fetch_valid()
    invalid = fetch_invalid()
    irrelevant = fetch_irrelevant()

    return valid, invalid, irrelevant

def write_input_file(abs_path, df, file_name):
        full_path = abs_path / file_name
        ext = full_path.suffix.lower()

        try:
            if ext == ".csv":
                df.to_csv(full_path, index=False)
            elif ext == ".xlsx":
                df.to_excel(full_path, index=False, engine="openpyxl")
            else:
                raise ValueError(f"Unsupported extension: '{ext}'")
        except Exception as e:
            st.error(f"❌ Failed to write {full_path}: {e} Check S3 connection")
            raise
        

def write_output_file(abs_path: Path, df: pd.DataFrame, file_name: str):
    """
    Write df to abs_path/file_name.csv
    """
    # Build the full path including the .csv extension
    full_path = abs_path / f"{file_name}.csv"
    df.to_csv(full_path, index=False)
    
    
async def write_input_to_s3(
    sfw_filename: str,
    sfw_df: pd.DataFrame,
    sector_filename: str,
    sector_df: pd.DataFrame,
    S3_INPUT_DIR_PATH: str = "../s3_bucket/s3_input",
):
    abs_path = Path(S3_INPUT_DIR_PATH).resolve()
    abs_path.mkdir(parents=True, exist_ok=True)

    # 1. rename both files concurrently
    renamed_sfw, renamed_sector = await asyncio.gather(
        rename_input_file(sfw_filename),
        rename_input_file(sector_filename),
    )

    loop = asyncio.get_running_loop()
    # 2. write both files concurrently in threadpool
    await asyncio.gather(
        loop.run_in_executor(None, write_input_file, abs_path, sfw_df, renamed_sfw),
        loop.run_in_executor(None, write_input_file, abs_path, sector_df, renamed_sector),
    )

    st.success(f"✅ Uploaded `{renamed_sfw}` and `{renamed_sector}` to {abs_path}")

def async_write_input_to_s3(*args, **kwargs):
    return asyncio.run(write_input_to_s3(*args, **kwargs))

# this is the rpoblem
async def write_output_to_s3(
    dfs: list[tuple[pd.DataFrame, str]],
    S3_OUTPUT_DIR_PATH: str = "../s3_bucket/s3_output",
):
    abs_path = Path(S3_OUTPUT_DIR_PATH).resolve()
    abs_path.mkdir(parents=True, exist_ok=True)

    # 1. kick off all the renames in parallel
    rename_tasks = [rename_output_file(fname) for _, fname in dfs]
    new_names = await asyncio.gather(*rename_tasks)

    loop = asyncio.get_running_loop()
    # 2. kick off all the writes in parallel
    write_tasks = [
        loop.run_in_executor(None, write_output_file, abs_path, df, new_name)
        for (df, _), new_name in zip(dfs, new_names)
    ]
    await asyncio.gather(*write_tasks)

    # 3. report success in UI
    for new_name in new_names:
        st.success(f"✅ Wrote `{new_name}` to {abs_path}")

def async_write_output_to_s3(dfs, **kwargs):
    """
    Synchronous entrypoint: runs the async writer under the hood.
    dfs should be a list of (DataFrame, original_filename) tuples.
    """
    return asyncio.run(write_output_to_s3(dfs, **kwargs))

