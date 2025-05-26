import streamlit as st
import pandas as pd
import asyncio
from typing import Optional, Tuple, Any, Callable
from pathlib import Path

# Import validation functions from separate file
from utils.input_validation import (
    FileValidationError,
    validate_file_non_empty,
    validate_sfw_schema,
    validate_sector_schema,
    has_mixed_skill_title_formats,
)

from backend_utils.course_file_preprocessing import build_course_skill_dataframe


# ===============================
# Sector File Processing Functions
# ===============================


def check_sector_requires_preprocessing(df: pd.DataFrame) -> bool:
    """
    Check if the sector file requires preprocessing.

    Args:
        df: The sector dataframe to check

    Returns:
        bool: True if preprocessing is required, False otherwise
    """
    # Check if there's a mix of string and list formats in Skill Title column
    if has_mixed_skill_title_formats(df):
        return True
    return False  # No preprocessing needed if no mixed formats detected


def run_preprocessing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply preprocessing steps to the sector file.

    Args:
        df: The sector dataframe to preprocess

    Returns:
        pd.DataFrame: The preprocessed dataframe
    """
    df = build_course_skill_dataframe(df)
    return df  # Placeholder - return original df for now


# ===============================
# File Validation Orchestrators
# ===============================


async def validate_sfw_file_input(uploaded) -> Tuple[bool, Optional[str]]:
    """
    Run SFW-specific validation checks and return detailed results.

    Returns:
        Tuple[bool, Optional[str]]: (is_valid, error_message)
    """
    validation_results = []
    error_messages = []

    # Run validation checks
    validation_checks = [
        ("File Size Check", validate_file_non_empty(uploaded)),
        ("SFW File Format Check", validate_sfw_schema(uploaded)),
    ]

    for check_name, check_coro in validation_checks:
        try:
            result = await check_coro
            validation_results.append(result)
        except FileValidationError as e:
            validation_results.append(False)
            error_messages.append(f"{check_name}: {str(e)}")
        except Exception as e:
            validation_results.append(False)
            error_messages.append(f"{check_name}: Unexpected error - {str(e)}")

    is_valid = all(validation_results)
    error_message = "; ".join(error_messages) if error_messages else None

    return is_valid, error_message


async def validate_sector_file_input(uploaded) -> Tuple[bool, Optional[str]]:
    """
    Run Sector file-specific validation checks and return detailed results.

    Returns:
        Tuple[bool, Optional[str]]: (is_valid, error_message)
    """
    validation_results = []
    error_messages = []

    # Run validation checks
    validation_checks = [
        ("File Size Check", validate_file_non_empty(uploaded)),
        ("Sector File Format Check", validate_sector_schema(uploaded)),
    ]

    for check_name, check_coro in validation_checks:
        try:
            result = await check_coro
            validation_results.append(result)
        except FileValidationError as e:
            validation_results.append(False)
            error_messages.append(f"{check_name}: {str(e)}")
        except Exception as e:
            validation_results.append(False)
            error_messages.append(f"{check_name}: Unexpected error - {str(e)}")

    is_valid = all(validation_results)
    error_message = "; ".join(error_messages) if error_messages else None

    return is_valid, error_message


# ===============================
# File Processing Utilities
# ===============================


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
    ext = Path(uploaded.name).suffix.lower()
    try:
        if ext == ".csv":
            df = pd.read_csv(uploaded)
        else:
            # For Excel files, read the first (and only) sheet
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


# ===============================
# Main Upload Functions
# ===============================


def upload_sfw_file() -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Handle SFW file upload with immediate validation.

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[str]]: (dataframe, filename) or (None, None)
    """
    uploaded = st.file_uploader(
        "Upload SFW File",
        type=["csv", "xlsx"],
        key="sfw_file",
        help="Upload your SFW file. Format: SFW_[SECTOR].xlsx (e.g., SFW_HR.xlsx)",
    )

    if uploaded is None:
        return None, None

    # Show file info
    st.write(f"📁 **File uploaded:** {uploaded.name}")
    st.write(f"📊 **File size:** {uploaded.size:,} bytes")

    # Validate file immediately upon upload
    with st.spinner("Validating SFW file..."):
        try:
            valid, error_message = asyncio.run(
                process_file_upload(uploaded, validate_sfw_file_input)
            )
        except RuntimeError:
            loop = asyncio.new_event_loop()
            valid, error_message = loop.run_until_complete(
                process_file_upload(uploaded, validate_sfw_file_input)
            )
            loop.close()

    if not valid:
        st.error(f"❌ **SFW file validation failed:**\n\n{error_message}")
        st.info("💡 **Please fix the issues above and upload your file again.**")
        return None, None

    # Read and display file
    df = read_uploaded_file(uploaded)
    if df is not None:
        display_file_preview(df, "SFW File")
        st.success("✅ **SFW file validated successfully!**")
        return df, uploaded.name

    return None, None


def upload_sector_file() -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Handle Sector file upload with immediate validation and optional preprocessing.

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[str]]: (dataframe, filename) or (None, None)
    """
    uploaded = st.file_uploader(
        "Upload Sector File",
        type=["xlsx"],
        key="sector_file",
        help="Upload your sector file. Format: [SECTOR]_[Full_Name]_sector_course_listing_curated.xlsx",
    )

    if uploaded is None:
        return None, None

    # Show file info
    st.write(f"📁 **File uploaded:** {uploaded.name}")
    st.write(f"📊 **File size:** {uploaded.size:,} bytes")

    # Initial validation
    with st.spinner("Validating sector file..."):
        try:
            valid, error_message = asyncio.run(
                process_file_upload(uploaded, validate_sector_file_input)
            )
        except RuntimeError:
            loop = asyncio.new_event_loop()
            valid, error_message = loop.run_until_complete(
                process_file_upload(uploaded, validate_sector_file_input)
            )
            loop.close()

    if not valid:
        st.error(f"❌ **Sector file validation failed:**\n\n{error_message}")
        st.info("💡 **Please fix the issues above and upload your file again.**")
        return None, None

    # Read file
    df = read_uploaded_file(uploaded)
    if df is None:
        return None, None

    # Check if preprocessing is required
    with st.spinner("Checking if preprocessing is needed..."):
        requires_preprocessing = check_sector_requires_preprocessing(df)

    if requires_preprocessing:
        st.info(
            "🔄 **Sector file requires preprocessing. Running preprocessing steps...**"
        )

        # Run preprocessing
        try:
            with st.spinner("Processing sector file..."):
                df = run_preprocessing(df)
            st.success("✅ **Preprocessing completed successfully!**")
        except Exception as e:
            st.error(f"❌ **Error during preprocessing:** {e}")
            return None, None

        st.success("✅ **Post-processing validation passed!**")
    else:
        st.info("ℹ️ **No preprocessing required for this sector file.**")

    # Display final preview
    display_file_preview(df, "Sector File")
    st.success("✅ **Sector file processed and validated successfully!**")

    return df, uploaded.name


# ===============================
# Legacy Function (for backward compatibility)
# ===============================


def upload_file(
    label: str, validator: Callable[[Any], asyncio.Future]
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Legacy function for backward compatibility.
    Consider using upload_sfw_file() or upload_sector_file() instead.
    """
    uploaded = st.file_uploader(f"Upload {label}", type=["csv", "xlsx"], key=label)

    if uploaded is None:
        return None, None

    try:
        valid, error_message = asyncio.run(process_file_upload(uploaded, validator))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        valid, error_message = loop.run_until_complete(
            process_file_upload(uploaded, validator)
        )
        loop.close()

    if not valid:
        st.error(f"Uploaded {label} failed validation: {error_message}")
        return None, None

    df = read_uploaded_file(uploaded)
    if df is not None:
        display_file_preview(df, label)
        return df, uploaded.name

    return None, None
