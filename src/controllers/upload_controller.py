"""
Upload controller module for handling file uploads and validation in the Streamlit interface.

This module provides functions to handle the upload and validation of both SFW
(Skills Framework) and Sector files through the Streamlit interface. It manages
the upload process, validation, preprocessing (for Sector files), and user feedback.
"""

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
    Handle the upload and validation of an SFW (Skills Framework) file.
    
    This function manages the complete upload process for an SFW file:
    1. Provides a file uploader widget
    2. Displays file information
    3. Validates the file using async validation
    4. Reads and displays a preview of the file
    5. Provides user feedback on the process
    
    Returns:
        Tuple[Optional[pd.DataFrame], Optional[str]]: A tuple containing:
            - Optional[pd.DataFrame]: The loaded DataFrame if successful, None otherwise
            - Optional[str]: The filename if successful, None otherwise
    
    Note:
        The function handles both synchronous and asynchronous validation,
        with fallback to a new event loop if needed. It provides clear user
        feedback through Streamlit's UI components.
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
    Handle the upload, validation, and preprocessing of a Sector file.
    
    This function manages the complete upload process for a Sector file:
    1. Provides a file uploader widget
    2. Displays file information
    3. Validates the file using async validation
    4. Checks if preprocessing is required
    5. Runs preprocessing if needed
    6. Reads and displays a preview of the file
    7. Provides user feedback on the process
    
    Returns:
        Tuple[Optional[pd.DataFrame], Optional[str]]: A tuple containing:
            - Optional[pd.DataFrame]: The processed DataFrame if successful, None otherwise
            - Optional[str]: The filename if successful, None otherwise
    
    Note:
        The function handles both synchronous and asynchronous validation,
        with fallback to a new event loop if needed. It provides clear user
        feedback through Streamlit's UI components and manages the preprocessing
        workflow for Sector files.
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
