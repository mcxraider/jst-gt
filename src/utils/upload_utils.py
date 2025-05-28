import pandas as pd
import streamlit as st
from typing import Tuple, Callable, Optional


def get_process_alias(process: str) -> str:
    """
    Extract the first two characters from a process string to create an alias.
    
    Args:
        process (str): The full process string
        
    Returns:
        str: A two-character alias derived from the process string
    """
    return process[:2]


def get_process(process: str) -> list[str]:
    """
    Extract the process name from a formatted process string.
    Removes the first 4 characters and last character from the input string.
    
    Args:
        process (str): The formatted process string
        
    Returns:
        list[str]: A list containing the extracted process name
    """
    return [process[4:-1]]


async def process_file_upload(
    uploaded, validator: Callable
) -> Tuple[bool, Optional[str]]:
    """
    Run file validation during file upload.

    Args:
        uploaded: The uploaded file object to validate
        validator (Callable): An async validation function that takes the uploaded file as input

    Returns:
        Tuple[bool, Optional[str]]: A tuple containing:
            - bool: Whether the file is valid
            - Optional[str]: Error message if validation fails, None if successful
    """
    return await validator(uploaded)


def read_uploaded_file(uploaded) -> Optional[pd.DataFrame]:
    """
    Read uploaded file into a pandas DataFrame.

    Args:
        uploaded: Streamlit uploaded file object

    Returns:
        Optional[pd.DataFrame]: The dataframe if successful, None if error occurs during reading
    """
    try:
        df = pd.read_excel(uploaded)
        return df
    except Exception as e:
        st.error(f"Error reading file {uploaded.name}: {e}")
        return None


def display_file_preview(df: pd.DataFrame, file_type: str) -> None:
    """
    Display a comprehensive preview of the uploaded file in Streamlit.
    
    This function shows:
    - A preview of the first few rows of the dataframe
    - Basic file statistics (total rows and columns)
    - Detailed column information including:
        - Column names
        - Data types
        - Non-null counts
        - Null counts
    
    Args:
        df (pd.DataFrame): The dataframe to preview
        file_type (str): The type/name of the file being previewed
    """
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
