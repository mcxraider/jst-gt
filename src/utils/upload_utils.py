import pandas as pd
import streamlit as st
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

    Args:
        uploaded: Streamlit uploaded file object

    Returns:
        pd.DataFrame or None: The dataframe if successful, None if error
    """
    try:
        df = pd.read_excel(uploaded)
        return df
    except Exception as e:
        st.error(f"Error reading file {uploaded.name}: {e}")
        return None


def display_file_preview(df: pd.DataFrame, file_type: str) -> None:
    """Display a preview of the uploaded file."""
    st.write(f"**Preview of {file_type}:**")
    st.dataframe(df.head())

    # Show basic info about the file
    st.write(f"**File Summary:**")
    st.write(f"- Total rows: {len(df):,}")
    st.write(f"- Total columns: {len(df.columns)}")

    # Show column info
    with st.expander("Click to see column details"):
        st.write("**Columns and Data Types:**")
        col_info = pd.DataFrame(
            {
                "Column Name": df.columns,
                "Data Type": [str(dtype) for dtype in df.dtypes],
                "Non-Null Count": [
                    f"{df[col].count():,} / {len(df):,}" for col in df.columns
                ],
                "Null Count": [f"{df[col].isnull().sum():,}" for col in df.columns],
            }
        )
        st.dataframe(col_info, use_container_width=True)
