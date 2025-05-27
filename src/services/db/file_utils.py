# services/db/file_utils.py
"""
File naming utilities for database operations.
Handles timestamp generation and file renaming patterns.
"""
import os
import datetime


async def rename_input_file(file_name: str) -> str:
    """
    Generate a timestamped input file name.

    Args:
        file_name (str): Original file name

    Returns:
        str: New file name with timestamp and '_input' suffix

    Example:
        "data.csv" -> "data_20231215_1430_input.csv"
    """
    base, ext = os.path.splitext(file_name)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    return f"{base}_{timestamp}_input{ext}"


async def rename_output_file(file_name: str) -> str:
    """
    Generate an output file name with '_output' suffix.

    Args:
        file_name (str): Original file name

    Returns:
        str: New file name with '_output' suffix

    Example:
        "results.csv" -> "results_output.csv"
    """
    base, ext = os.path.splitext(file_name)
    return f"{base}_output{ext}"
