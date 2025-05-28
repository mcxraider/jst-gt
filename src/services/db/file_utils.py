# services/db/file_utils.py
"""
File naming utilities for database operations.
Handles timestamp generation and file renaming patterns.

This module provides utility functions for managing file names in the database
operations. It includes functions for generating timestamped names for input
files and standardized names for output files, ensuring consistent naming
conventions across the application.
"""
import os
import datetime


async def rename_input_file(file_name: str) -> str:
    """
    Generate a timestamped input file name.
    
    Creates a new file name by adding a timestamp and '_input' suffix to the
    original file name. The timestamp format is YYYYMMDD_HHMM.
    
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
    
    Creates a new file name by adding an '_output' suffix to the original
    file name, maintaining the original extension.
    
    Args:
        file_name (str): Original file name
        
    Returns:
        str: New file name with '_output' suffix
        
    Example:
        "results.csv" -> "results_output.csv"
    """
    base, ext = os.path.splitext(file_name)
    return f"{base}_output{ext}"
