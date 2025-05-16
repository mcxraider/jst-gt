import streamlit as st
import pandas as pd
import os
import asyncio
from pathlib import Path
import time
import datetime


async def rename_input_file(file_name: str) -> str:
    """
    Asynchronously renames the file by appending a timestamp and 'input' before the file extension.
    """
    base, ext = os.path.splitext(file_name)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    new_name = f"{base}_{timestamp}_input{ext}"
    return new_name



async def rename_output_file(file_name: str) -> str:
    """
    Asynchronously renames the file by appending a timestamp and 'output' before the file extension.
    """
    base, ext = os.path.splitext(file_name)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    new_name = f"{base}_{timestamp}_output{ext}"
    return new_name


def delete_all_s3(dir):
    time.sleep(5)
    # Iterate and clear contents of each bucket folder
    for bucket in dir.iterdir():
        if bucket.is_dir():
            for item in bucket.iterdir():
                try:
                    if item.is_file():
                        item.unlink()
                    elif item.is_dir():
                        for sub in item.iterdir():
                            if sub.is_file():
                                sub.unlink()
                except Exception as e:
                    st.warning(f"Failed to delete {item}: {e}")


async def wipe_db():
    """Completely wipe contents of each folder in the s3_bucket directory only if needed."""
    # Only wipe if a CSV or checkpoint has been processed
    if not (
        st.session_state.get("csv_yes", False) or st.session_state.get("pkl_yes", False)
    ):
        return

    # Indicate the wipe and reset flags

    st.info("Wiping database now…")
    base_dir = Path("../s3_bucket")

    # logic below should be replaced with this function here
    delete_all_s3(base_dir)

    st.session_state["csv_yes"] = False
    st.session_state["pkl_yes"] = False


def _fetch_df(path: str) -> tuple[pd.DataFrame, str]:
    """
    Helper to load a CSV and return the DataFrame along with its base filename (without extension).
    """
    filepath = Path(path)
    df = pd.read_csv(filepath)
    name = filepath.stem
    return df, name


def fetch_valid(
    path: str = "../temp_output/AirTransport_all_valid_skill_pl_20250228_0103.csv",
) -> tuple[pd.DataFrame, str]:
    """
    Fetch the 'valid' skills CSV.
    """
    return _fetch_df(path)


def fetch_irrelevant(
    path: str = "../temp_output/AirTransport_irrelevant_skills.csv",
) -> tuple[pd.DataFrame, str]:
    """
    Fetch the 'irrelevant' skills CSV.
    """
    return _fetch_df(path)


def fetch_invalid(
    path: str = "../temp_output/AirTransport_r2_invalid_skill_pl_20250228_0103.csv",
) -> tuple[pd.DataFrame, str]:
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
        loop.run_in_executor(
            None, write_input_file, abs_path, sector_df, renamed_sector
        ),
    )

    st.success(f"✅ Uploaded input files to S3")


def async_write_input_to_s3(*args, **kwargs):
    return asyncio.run(write_input_to_s3(*args, **kwargs))


async def write_output_to_s3(
    dfs: list[tuple[pd.DataFrame, str]],
    S3_OUTPUT_DIR_PATH: str = "../s3_bucket/s3_output",
):
    abs_path = Path(S3_OUTPUT_DIR_PATH).resolve()
    abs_path.mkdir(parents=True, exist_ok=True)


    # Debug check
    for i, item in enumerate(dfs):
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError(
                f"[ERROR] dfs[{i}] must be a tuple (DataFrame, str), but got: {item}"
            )

    # 1. Rename tasks in parallel
    rename_tasks = [rename_output_file(fname) for _, fname in dfs]
    new_names = await asyncio.gather(*rename_tasks)

    loop = asyncio.get_running_loop()
    write_tasks = [
        loop.run_in_executor(None, write_output_file, abs_path, df, new_name)
        for (df, _), new_name in zip(dfs, new_names)
    ]
    await asyncio.gather(*write_tasks)


    st.success(f"✅ Wrote all {len(dfs)} output files to S3")


def async_write_output_to_s3(dfs, **kwargs):
    """
    Synchronous entrypoint: runs the async writer under the hood.
    dfs should be a list of (DataFrame, original_filename) tuples.
    """
    return asyncio.run(write_output_to_s3(dfs, **kwargs))
