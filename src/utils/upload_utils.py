import pandas as pd
import streamlit as st
from pathlib import Path
from typing import Tuple, Callable, Optional


def get_process_alias(process: str) -> str:
    alias, name = process.split(" (", 1)
    return alias


def get_process(process: str) -> list[str]:
    _, name = process.split(" (", 1)
    name = name.rstrip(")")
    return [name]


async def process_file_upload(
    uploaded, validator: Callable
) -> Tuple[bool, Optional[str]]:
    """
    Run file validation during file upload.

    Returns:
        Tuple[bool, Optional[str]]: (is_valid, error_message)
    """
    return await validator(uploaded)


def read_uploaded_file(uploaded) -> Optional[pd.DataFrame]:
    """
    Read uploaded file into a pandas DataFrame.
    Supports both Excel (.xlsx, .xls) and CSV (.csv) formats.

    Args:
        uploaded: Streamlit uploaded file object

    Returns:
        pd.DataFrame or None: The dataframe if successful, None if error
    """
    try:
        file_ext = Path(uploaded.name).suffix.lower()

        if file_ext in [".xlsx", ".xls"]:
            df = pd.read_excel(uploaded)
        elif file_ext == ".csv":
            df = pd.read_csv(uploaded)
        else:
            st.error(
                f"Unsupported file format: {file_ext}. Please upload an Excel or CSV file."
            )
            return None

        return df
    except Exception as e:
        st.error(f"Error reading file {uploaded.name}: {e}")
        return None


def display_file_preview(df: pd.DataFrame, file_type: str) -> None:
    """Display a preview of the uploaded file."""
    st.write(f"**Preview of {file_type}:**")
    st.dataframe(df.head())
