import streamlit as st
import pandas as pd
import os
import asyncio
from typing import Optional, Tuple, List, Any, Callable
from pathlib import Path
import time
import datetime
import importlib

# -------------------------------
# Async Utilities
# -------------------------------


def delete_all_s3():
    pass


async def wipe_db():
    """Completely wipe contents of each folder in the s3_bucket directory only if needed."""
    # Only wipe if a CSV or checkpoint has been processed
    if not (
        st.session_state.get("csv_yes", False) or st.session_state.get("pkl_yes", False)
    ):
        return

    # Indicate the wipe and reset flags
    st.write("Wiping database now…")
    time.sleep(5)
    st.session_state["csv_yes"] = False
    st.session_state["pkl_yes"] = False

    base_dir = Path("../s3_bucket")
    if not base_dir.exists() or not base_dir.is_dir():
        return

    # logic below should be replaced with this function here
    delete_all_s3()

    # Iterate and clear contents of each bucket folder
    for bucket in base_dir.iterdir():
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


# TODO:
# define more validators here


async def validate_file_non_empty(uploaded) -> bool:
    """Ensure the uploaded file is not empty."""
    uploaded.seek(0, os.SEEK_END)
    size = uploaded.tell()
    uploaded.seek(0)
    return size > 0


# need to validate the schema carefully
async def validate_sfw_schema(uploaded) -> bool:
    return True


# need to validate the schema carefully
async def validate_sector_schema(uploaded) -> bool:
    return True


async def validate_sfw_file_input(uploaded) -> bool:
    """Run SFW-specific validation checks concurrently and return overall result."""
    checks = [validate_sfw_schema(uploaded), validate_file_non_empty(uploaded)]
    results = await asyncio.gather(*checks)
    return all(results)


async def validate_sector_file_input(uploaded) -> bool:
    """Run Sector file-specific validation checks concurrently and return overall result."""
    checks = [validate_sector_schema(uploaded), validate_file_non_empty(uploaded)]
    results = await asyncio.gather(*checks)
    return all(results)


async def process_file_upload(uploaded, validator: Callable) -> bool:
    """Run file validation and wipe_db concurrently during file upload."""
    valid, _ = await asyncio.gather(validator(uploaded), wipe_db())
    return valid


# Sync Wrapper
def upload_file(
    label: str, validator: Callable[[Any], asyncio.Future]
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Display file uploader, validate via provided async validator, and read file."""
    uploaded = st.file_uploader(
        f"Upload {label}", type=["csv", "xlsx"], key=label
    )
    if uploaded is None:
        return None, None

    try:
        valid = asyncio.run(process_file_upload(uploaded, validator))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        valid = loop.run_until_complete(process_file_upload(uploaded, validator))
        loop.close()

    if not valid:
        st.error(f"Uploaded {label} failed validation.")
        return None, None

    ext = Path(uploaded.name).suffix.lower()
    try:
        if ext == ".csv":
            df = pd.read_csv(uploaded)
        else:
            df = pd.read_excel(uploaded)
        st.success(f"{label} loaded and validated successfully.")
        st.write(f"**Preview of {label.lower()}:**")
        st.dataframe(df.head())
        return df, uploaded.name
    except Exception as e:
        st.error(f"Error reading {label.lower()}: {e}")
        return None, None


async def rename_input_file(file_name: str) -> str:
    """
    Asynchronously renames the file by appending a timestamp and 'input' before the file extension.
    """
    base, ext = os.path.splitext(file_name)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    new_name = f"{base}_{timestamp}_input{ext}"
    return new_name


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
    S3_DIR_PATH = "../s3_bucket/s3_input",
):
    renamed_sfw, renamed_sector = await asyncio.gather(
        rename_input_file(sfw_filename), rename_input_file(sector_filename)
    )

    write_input_to_s3(sfw_df, sector_df, S3_DIR_PATH, renamed_sfw, renamed_sector)

    st.write(
        f"Sending files to s3_input"
    )
    return

def insert_input_to_s3_sync(*args, **kwargs):
    return asyncio.run(insert_input_to_s3(*args, **kwargs))
