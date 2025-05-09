import streamlit as st
import pandas as pd
import os
import asyncio
from typing import Optional, Tuple, List, Any, Callable
from pathlib import Path
import datetime

from utils.db import wipe_db


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
    label: str, 
    validator: Callable[[Any], asyncio.Future]
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Display file uploader, validate via provided async validator, and read file."""
    uploaded = st.file_uploader(f"Upload {label}", type=["csv", "xlsx"], key=label)
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


