import streamlit as st
import pandas as pd
import os
import asyncio
from pathlib import Path
import time
import pickle
import datetime
from backend_utils.config import (
    base_dir,
    output_path,
    checkpoint_path,
    input_data_path,
    intermediate_output_path,
    misc_output_path,
)


output_path = Path(output_path)
base_dir = Path(base_dir)
checkpoint_path = Path(checkpoint_path)
input_data_path = Path(input_data_path)
intermediate_output_path = Path(intermediate_output_path)
misc_output_path = Path(misc_output_path)


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
    new_name = f"{base}_output{ext}"
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


def wipe_db(caption):
    """Completely wipe contents of each folder in the s3_bucket directory only if needed."""

    caption.caption("[Status] Erasing data from previous run...")

    # Only wipe if a CSV or checkpoint has been processed
    if not (
        st.session_state.get("csv_yes", False) or st.session_state.get("pkl_yes", False)
    ):
        return
    # logic below should be replaced with this function here
    delete_all_s3(base_dir)

    st.session_state["csv_yes"] = False
    st.session_state["pkl_yes"] = False


def _fetch_df(path: Path) -> tuple[pd.DataFrame, str]:
    """
    Helper to load a CSV and return the DataFrame along with its base filename (without extension).
    """
    df = pd.read_csv(path)
    name = path.stem
    return df, name


def fetch_by_prefix(prefix: str) -> tuple[pd.DataFrame, str]:
    """
    Fetches the first CSV file in BASE_DIR starting with the given prefix.
    """
    matches = list(output_path.glob(f"{prefix}*.csv"))
    if not matches:
        raise FileNotFoundError(
            f"No file starting with '{prefix}' found in {output_path}"
        )
    return _fetch_df(matches[0])


def fetch_valid() -> tuple[pd.DataFrame, str]:
    return fetch_by_prefix("Valid Skills")


def fetch_invalid() -> tuple[pd.DataFrame, str]:
    return fetch_by_prefix("Invalid Skills")


def fetch_all_tagged() -> tuple[pd.DataFrame, str]:
    return fetch_by_prefix("All Tagged Skills")


def fetch_completed_output():
    valid = fetch_valid()
    invalid = fetch_invalid()
    all_tagged = fetch_all_tagged()
    return valid, invalid, all_tagged


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
):

    abs_path = input_data_path.resolve()
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


def async_write_input_to_s3(caption, *args, **kwargs):
    caption.caption("[Status] Saving input files to database...")
    return asyncio.run(write_input_to_s3(*args, **kwargs))


async def write_output_to_s3(
    dfs: list[tuple[pd.DataFrame, str]],
):
    abs_path = output_path.resolve()
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


def async_write_output_to_s3(caption, dfs):
    """
    Synchronous entrypoint: runs the async writer under the hood.
    dfs should be a list of (DataFrame, original_filename) tuples.
    """
    caption.caption("[Status] Results are ready, saving files to database...")
    return asyncio.run(write_output_to_s3(dfs))


def load_checkpoint_metadata():
    """
    Loads metadata from the checkpoint .pkl file.
    """
    ckpt_dir = checkpoint_path
    pkl_files = list(ckpt_dir.glob("*.pkl"))

    if not pkl_files:
        raise FileNotFoundError("No checkpoint file found in the directory.")
    if len(pkl_files) > 1:
        raise RuntimeError("Multiple checkpoint files found. Expected only one.")

    ckpt_file = pkl_files[0]

    with open(ckpt_file, "rb") as f:
        data = pickle.load(f)

    metadata = {
        "round": data.get("round"),
        "progress": data.get("progress"),
        "sector": data.get("sector"),
    }

    return metadata


def check_pkl_existence() -> bool:
    """
    Check whether any .pkl files exist in the checkpoint directory.
    """
    return checkpoint_path.exists() and any(checkpoint_path.glob("*.pkl"))


def load_sfw_file() -> pd.DataFrame:

    input_dir = input_data_path
    for fp in input_dir.glob("*.xlsx"):
        if fp.name.startswith("SFW"):
            return pd.read_excel(fp)
    raise FileNotFoundError(f"No file starting with 'SFW' in {input_data_path}")


def load_sector_file(cols=None) -> pd.DataFrame:
    """
    Finds the single Excel file in `input_data_path` whose
    name does NOT start with 'SFW', prints its name, and returns it.
    """

    input_dir = input_data_path
    for fp in input_dir.glob("*.xlsx"):
        if not fp.name.startswith("SFW"):
            return pd.read_excel(
                fp,
                usecols=cols,
            )
    raise FileNotFoundError(f"No sector file found in {input_data_path}")


def load_r1_invalid() -> pd.DataFrame:
    """
    Finds the single CSV in `intermediate_output_path` whose
    name contains 'r1_irrelevant', reads it, and returns it.
    """
    intermediate_dir = intermediate_output_path
    for fp in intermediate_dir.glob("*.csv"):
        if "r1_invalid" in fp.name:
            print(f"Loading r1 invalid file named: {fp.name}")
            return pd.read_csv(fp, low_memory=False, encoding="utf-8")
    raise FileNotFoundError(
        f"No file containing 'r1_invalid' in {intermediate_output_path}"
    )


def load_r1_valid():
    intermediate_dir = intermediate_output_path
    for fp in intermediate_dir.glob("*.csv"):
        if "r1_valid" in fp.name:
            return pd.read_csv(fp, low_memory=False, encoding="utf-8")
    raise FileNotFoundError(
        f"No file containing 'r1_valid' in {intermediate_output_path}"
    )


def write_r1_invalid_to_s3(df: pd.DataFrame, target_sector_alias: str):
    path = f"{intermediate_output_path}/{target_sector_alias}_r1_invalid_skill_pl.csv"
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"Saved invalid R1 output to {path}")


def write_r1_valid_to_s3(df: pd.DataFrame, target_sector_alias: str):
    path = f"{intermediate_output_path}/{target_sector_alias}_r1_valid_skill_pl.csv"
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"Saved valid R1 output to {path}")


def write_irrelevant_to_s3(df: pd.DataFrame, target_sector_alias: str):
    path = f"{intermediate_output_path}/{target_sector_alias}_r1_irrelevant.csv"
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"Saved irrelevant output to {path}")


def write_r2_raw_to_s3(df: pd.DataFrame, target_sector_alias: str):
    path = f"{misc_output_path}/{target_sector_alias}_course_skill_pl_rac_raw.csv"
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"Saved R2 raw output to {path}")
