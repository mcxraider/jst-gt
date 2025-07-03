import streamlit as st
import pandas as pd
import asyncio
from typing import Optional, Tuple

from services.ingestion.sector_file_processing import (
    check_sector_requires_preprocessing,
    run_preprocessing,
)

from utils.upload_utils import *
from services.validation.input_validation import *


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
    st.write(
        f"📁 **File uploaded:** {uploaded.name} with size: {uploaded.size:,} bytes"
    )

    # Validate file again upon upload
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
        "Upload Sector File (Note: Sector file is large and takes awhile longer to process)",
        type=["xlsx"],
        key="sector_file",
        help="Upload your sector file. Format: [SECTOR]_[Full_Name]_sector_course_listing_curated.xlsx",
    )

    if uploaded is None:
        return None, None

    # Show file info
    st.write(
        f"📁 **File uploaded:** {uploaded.name} with size: {uploaded.size:,} bytes"
    )

    # Initial validation
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
