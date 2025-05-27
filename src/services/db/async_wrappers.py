# services/db/async_wrappers.py
"""
Asynchronous wrapper functions for database operations with Streamlit integration.
Provides status updates and handles async execution in Streamlit context.
"""
import asyncio
import streamlit as st

from .data_writers import write_input_to_s3, write_output_to_s3


def async_write_input_to_s3(caption, *args, **kwargs):
    """
    Synchronous wrapper for async input file writing with status updates.

    Args:
        caption: Streamlit caption object for status updates
        *args: Arguments to pass to write_input_to_s3
        **kwargs: Keyword arguments to pass to write_input_to_s3

    Returns:
        Result of write_input_to_s3 execution
    """
    caption.caption("[Status] Saving input files to database...")
    return asyncio.run(write_input_to_s3(*args, **kwargs))


def async_write_output_to_s3(caption, dfs):
    """
    Synchronous wrapper for async output file writing with status updates.

    Args:
        caption: Streamlit caption object for status updates
        dfs (list): List of (DataFrame, filename) tuples to write

    Returns:
        Result of write_output_to_s3 execution

    Note:
        Displays success message with file count upon completion.
    """
    caption.caption("[Status] Results are ready, saving files to database...")
    result = asyncio.run(write_output_to_s3(dfs))
    st.success(f"✅ Wrote all {len(dfs)} output files to S3")
    return result
