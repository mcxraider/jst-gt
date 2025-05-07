import streamlit as st
import pandas as pd
import os
import asyncio
from typing import Optional, Tuple, List, Any, Callable
from pathlib import Path
import time
import datetime
import importlib

from utils.input_handler import rename_input_file


def write_input_to_s3(sfw_df, sector_df, s3_input_path, renamed_sfw, renamed_sector):
    """
    Writes the two DataFrames to files in the provided s3_input_path.
    Supports .csv, .xlsx, and .xls formats based on the renamed filenames.
    """
    abs_path = Path(s3_input_path).resolve()
    abs_path.mkdir(parents=True, exist_ok=True)

    def write_file(df, file_name):
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

    write_file(sfw_df, renamed_sfw)
    write_file(sector_df, renamed_sector)


async def insert_input_to_s3(
    sfw_filename,
    sfw_df,
    sector_filename,
    sector_df,
    S3_DIR_PATH="../s3_bucket/s3_input",
):
    renamed_sfw, renamed_sector = await asyncio.gather(
        rename_input_file(sfw_filename), rename_input_file(sector_filename)
    )

    write_input_to_s3(sfw_df, sector_df, S3_DIR_PATH, renamed_sfw, renamed_sector)

    st.write(f"Sending files to s3_input")
    return


def insert_input_to_s3_sync(*args, **kwargs):
    return asyncio.run(insert_input_to_s3(*args, **kwargs))
