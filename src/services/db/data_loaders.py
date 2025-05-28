# services/db/data_loaders.py
"""
Data loading functions for various file types and patterns.
Handles loading of input files, checkpoint data, and processed results.

This module provides a comprehensive set of functions for loading data from
various sources including CSV files, Excel files, and pickle files. It supports
both local and S3 storage, with specific functions for different data types
and processing stages.
"""
import pandas as pd
from pathlib import Path

from config import (
    INPUT_DATA_PATH,
    CHECKPOINT_PATH,
    OUTPUT_PATH,
    INTERMEDIATE_OUTPUT_PATH,
)
from services.storage import load_csv, load_excel, load_pickle, list_files


def _fetch_df(path: str) -> tuple[pd.DataFrame, str]:
    """
    Load DataFrame from CSV file and extract name from path.
    
    Internal helper function that handles the common CSV loading pattern
    and filename extraction.
    
    Args:
        path (str): File path to load
        
    Returns:
        tuple: (DataFrame, file_name_without_extension)
    """
    df = load_csv(path)
    name = Path(path).stem
    return df, name


def fetch_by_prefix(prefix: str) -> tuple[pd.DataFrame, str]:
    """
    Find and load the first CSV file matching a prefix in the output directory.
    
    Searches the output directory for files matching the given prefix
    and loads the first match into a DataFrame.
    
    Args:
        prefix (str): File name prefix to search for
        
    Returns:
        tuple: (DataFrame, file_name_without_extension)
        
    Raises:
        FileNotFoundError: If no matching file is found
    """
    matches = list_files(OUTPUT_PATH, f"{prefix}*.csv")
    if not matches:
        raise FileNotFoundError(
            f"No file starting with '{prefix}' found in {OUTPUT_PATH}"
        )
    return _fetch_df(matches[0])


def fetch_valid() -> tuple[pd.DataFrame, str]:
    """
    Load the valid skills output file.
    
    Specifically loads the CSV file containing skills that were
    successfully validated during processing.
    
    Returns:
        tuple: (DataFrame, file_name_without_extension)
        
    Raises:
        FileNotFoundError: If no valid skills file is found
    """
    return fetch_by_prefix("Valid Skills")


def fetch_invalid() -> tuple[pd.DataFrame, str]:
    """
    Load the invalid skills output file.
    
    Specifically loads the CSV file containing skills that were
    marked as invalid during processing.
    
    Returns:
        tuple: (DataFrame, file_name_without_extension)
        
    Raises:
        FileNotFoundError: If no invalid skills file is found
    """
    return fetch_by_prefix("Invalid Skills")


def fetch_all_tagged() -> tuple[pd.DataFrame, str]:
    """
    Load the all tagged skills output file.
    
    Specifically loads the CSV file containing all skills that
    were processed, regardless of validation status.
    
    Returns:
        tuple: (DataFrame, file_name_without_extension)
        
    Raises:
        FileNotFoundError: If no all tagged skills file is found
    """
    return fetch_by_prefix("All Tagged Skills")


def fetch_completed_output():
    """
    Load all completed output files (valid, invalid, and all tagged).
    
    Convenience function that loads all three types of output files
    in a single call.
    
    Returns:
        tuple: (valid_data, invalid_data, all_tagged_data)
        Each element is a tuple of (DataFrame, file_name)
        
    Raises:
        FileNotFoundError: If any of the required files are not found
    """
    valid = fetch_valid()
    invalid = fetch_invalid()
    all_tagged = fetch_all_tagged()
    return valid, invalid, all_tagged


def load_checkpoint_metadata():
    """
    Load checkpoint metadata from pickle file.
    
    Extracts essential metadata from the most recent checkpoint file,
    including processing round, progress, and sector information.
    
    Returns:
        dict: Checkpoint metadata containing:
            - round: Current processing round
            - progress: Processing progress (0-1)
            - sector: Current sector being processed
            
    Raises:
        FileNotFoundError: If no checkpoint file is found
        RuntimeError: If multiple checkpoint files are found
    """
    pkl_files = list_files(CHECKPOINT_PATH, "*.pkl")
    if not pkl_files:
        raise FileNotFoundError("No checkpoint file found in the directory.")
    if len(pkl_files) > 1:
        raise RuntimeError("Multiple checkpoint files found. Expected only one.")

    data = load_pickle(pkl_files[0])
    metadata = {
        "round": data.get("round"),
        "progress": data.get("progress"),
        "sector": data.get("sector"),
    }
    return metadata


def check_pkl_existence() -> bool:
    """
    Check if any pickle files exist in the checkpoint directory.
    
    Returns:
        bool: True if pickle files exist, False otherwise
    """
    return bool(list_files(CHECKPOINT_PATH, "*.pkl"))


def check_output_existence() -> bool:
    """
    Check if exactly 3 output CSV files exist in the output directory.
    
    Verifies the presence of all required output files:
    - Valid skills
    - Invalid skills
    - All tagged skills
    
    Returns:
        bool: True if exactly three .csv files exist, False otherwise
    """
    csv_files = list_files(OUTPUT_PATH, "*.csv")
    return len(csv_files) == 3


def load_sfw_file() -> pd.DataFrame:
    """
    Load the SFW (Skills Framework) Excel file from input directory.
    
    Specifically loads the Skills Framework workbook that contains
    the reference data for skill validation.
    
    Returns:
        pd.DataFrame: Loaded SFW data
        
    Raises:
        FileNotFoundError: If no file starting with 'SFW' is found
    """
    files = list_files(INPUT_DATA_PATH, "*.xlsx")
    for fp in files:
        if Path(fp).name.startswith("SFW"):
            return load_excel(fp)
    raise FileNotFoundError(f"No file starting with 'SFW' in {INPUT_DATA_PATH}")


def load_sector_file(cols=None) -> pd.DataFrame:
    """
    Load the sector Excel file (non-SFW file) from input directory.
    
    Loads the sector-specific data file that contains course and
    skill information for processing.
    
    Args:
        cols (list, optional): Specific columns to load. Defaults to None.
        
    Returns:
        pd.DataFrame: Loaded sector data
        
    Raises:
        FileNotFoundError: If no sector file is found
    """
    files = list_files(INPUT_DATA_PATH, "*.xlsx")
    for fp in files:
        if not Path(fp).name.startswith("SFW"):
            return load_excel(fp, usecols=cols)
    raise FileNotFoundError(f"No sector file found in {INPUT_DATA_PATH}")


def load_r1_invalid() -> pd.DataFrame:
    """
    Load the round 1 invalid skills CSV file from intermediate output directory.
    
    Specifically loads the results of round 1 processing that were
    marked as invalid for further processing in round 2.
    
    Returns:
        pd.DataFrame: Loaded R1 invalid data
        
    Raises:
        FileNotFoundError: If no R1 invalid file is found
    """
    files = list_files(INTERMEDIATE_OUTPUT_PATH, "*.csv")
    for fp in files:
        if "r1_invalid" in Path(fp).name:
            return load_csv(fp)
    raise FileNotFoundError(
        f"No file containing 'r1_invalid' in {INTERMEDIATE_OUTPUT_PATH}"
    )


def load_r1_valid():
    """
    Load the round 1 valid skills CSV file from intermediate output directory.
    
    Specifically loads the results of round 1 processing that were
    successfully validated and don't require round 2 processing.
    
    Returns:
        pd.DataFrame: Loaded R1 valid data
        
    Raises:
        FileNotFoundError: If no R1 valid file is found
    """
    files = list_files(INTERMEDIATE_OUTPUT_PATH, "*.csv")
    for fp in files:
        if "r1_valid" in Path(fp).name:
            return load_csv(fp)
    raise FileNotFoundError(
        f"No file containing 'r1_valid' in {INTERMEDIATE_OUTPUT_PATH}"
    )
