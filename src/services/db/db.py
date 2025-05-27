# services/db.py
import streamlit as st
import pandas as pd
import os
import asyncio
import datetime
from pathlib import Path

from config import (
    base_dir, output_path, checkpoint_path, input_data_path,
    intermediate_output_path, misc_output_path
)
from services.storage.storage import (
    save_csv, load_csv, save_excel, load_excel, load_pickle,
    list_files, delete_all
)

async def rename_input_file(file_name: str) -> str:
    base, ext = os.path.splitext(file_name)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    return f"{base}_{timestamp}_input{ext}"

async def rename_output_file(file_name: str) -> str:
    base, ext = os.path.splitext(file_name)
    return f"{base}_output{ext}"

def wipe_db(caption):
    caption.caption("[Status] Erasing data from previous run...")
    if not (st.session_state.get("csv_yes", False) or st.session_state.get("pkl_yes", False)):
        return
    delete_all(base_dir)
    st.session_state["csv_yes"] = False
    st.session_state["pkl_yes"] = False

def _fetch_df(path: str) -> tuple[pd.DataFrame, str]:
    df = load_csv(path)
    name = Path(path).stem
    return df, name

def fetch_by_prefix(prefix: str) -> tuple[pd.DataFrame, str]:
    matches = list_files(output_path, f"{prefix}*.csv")
    if not matches:
        raise FileNotFoundError(f"No file starting with '{prefix}' found in {output_path}")
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
    path = Path(abs_path) / file_name
    ext = path.suffix.lower()
    if ext == ".csv":
        save_csv(df, str(path))
    elif ext == ".xlsx":
        save_excel(df, str(path))
    else:
        raise ValueError(f"Unsupported extension: '{ext}'")

def write_output_file(abs_path: str, df: pd.DataFrame, file_name: str):
    path = Path(abs_path) / f"{file_name}.csv"
    save_csv(df, str(path))

async def write_input_to_s3(sfw_filename: str, sfw_df: pd.DataFrame, sector_filename: str, sector_df: pd.DataFrame):
    abs_path = input_data_path
    os.makedirs(abs_path, exist_ok=True)
    renamed_sfw, renamed_sector = await asyncio.gather(
        rename_input_file(sfw_filename),
        rename_input_file(sector_filename),
    )
    loop = asyncio.get_running_loop()
    await asyncio.gather(
        loop.run_in_executor(None, write_input_file, abs_path, sfw_df, renamed_sfw),
        loop.run_in_executor(None, write_input_file, abs_path, sector_df, renamed_sector),
    )

def async_write_input_to_s3(caption, *args, **kwargs):
    caption.caption("[Status] Saving input files to database...")
    return asyncio.run(write_input_to_s3(*args, **kwargs))

async def write_output_to_s3(dfs: list[tuple[pd.DataFrame, str]]):
    abs_path = output_path
    os.makedirs(abs_path, exist_ok=True)
    for i, item in enumerate(dfs):
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError(f"[ERROR] dfs[{i}] must be a tuple (DataFrame, str), but got: {item}")
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
    caption.caption("[Status] Results are ready, saving files to database...")
    return asyncio.run(write_output_to_s3(dfs))

def load_checkpoint_metadata():
    pkl_files = list_files(checkpoint_path, "*.pkl")
    if not pkl_files:
        raise FileNotFoundError("No checkpoint file found in the directory.")
    if len(pkl_files) > 1:
        raise RuntimeError("Multiple checkpoint files found. Expected only one.")
    data = load_pickle(pkl_files[0])
    metadata = {
        "round": data.get("round"),
        "progress": data.get("progress"),
        "sector": data.get("sector"),
    }
    return metadata

def check_pkl_existence() -> bool:
    return bool(list_files(checkpoint_path, "*.pkl"))

def load_sfw_file() -> pd.DataFrame:
    files = list_files(input_data_path, "*.xlsx")
    for fp in files:
        if Path(fp).name.startswith("SFW"):
            return load_excel(fp)
    raise FileNotFoundError(f"No file starting with 'SFW' in {input_data_path}")

def load_sector_file(cols=None) -> pd.DataFrame:
    files = list_files(input_data_path, "*.xlsx")
    for fp in files:
        if not Path(fp).name.startswith("SFW"):
            return load_excel(fp, usecols=cols)
    raise FileNotFoundError(f"No sector file found in {input_data_path}")

def load_r1_invalid() -> pd.DataFrame:
    files = list_files(intermediate_output_path, "*.csv")
    for fp in files:
        if "r1_invalid" in Path(fp).name:
            print(f"Loading r1 invalid file named: {Path(fp).name}")
            return load_csv(fp)
    raise FileNotFoundError(f"No file containing 'r1_invalid' in {intermediate_output_path}")

def load_r1_valid():
    files = list_files(intermediate_output_path, "*.csv")
    for fp in files:
        if "r1_valid" in Path(fp).name:
            return load_csv(fp)
    raise FileNotFoundError(f"No file containing 'r1_valid' in {intermediate_output_path}")

def write_r1_invalid_to_s3(df: pd.DataFrame, target_sector_alias: str):
    path = f"{intermediate_output_path}/{target_sector_alias}_r1_invalid_skill_pl.csv"
    save_csv(df, path)
    print(f"Saved invalid R1 output to {path}")

def write_r1_valid_to_s3(df: pd.DataFrame, target_sector_alias: str):
    path = f"{intermediate_output_path}/{target_sector_alias}_r1_valid_skill_pl.csv"
    save_csv(df, path)
    print(f"Saved valid R1 output to {path}")

def write_irrelevant_to_s3(df: pd.DataFrame, target_sector_alias: str):
    path = f"{intermediate_output_path}/{target_sector_alias}_r1_irrelevant.csv"
    save_csv(df, path)
    print(f"Saved irrelevant output to {path}")

def write_r2_raw_to_s3(df: pd.DataFrame, target_sector_alias: str):
    path = f"{misc_output_path}/{target_sector_alias}_course_skill_pl_rac_raw.csv"
    save_csv(df, path)
    print(f"Saved R2 raw output to {path}")

