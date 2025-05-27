# services/db/data_loaders.py
"""
Data loading functions for various file types and patterns.
Handles loading of input files, checkpoint data, and processed results.
"""
import pandas as pd
from pathlib import Path

from config import (
    input_data_path,
    checkpoint_path,
    output_path,
    intermediate_output_path,
)
from services.storage import load_csv, load_excel, load_pickle, list_files


def _fetch_df(path: str) -> tuple[pd.DataFrame, str]:
    """
    Load DataFrame from CSV file and extract name from path.

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

    Args:
        prefix (str): File name prefix to search for

    Returns:
        tuple: (DataFrame, file_name_without_extension)

    Raises:
        FileNotFoundError: If no matching file is found
    """
    matches = list_files(output_path, f"{prefix}*.csv")
    if not matches:
        raise FileNotFoundError(
            f"No file starting with '{prefix}' found in {output_path}"
        )
    return _fetch_df(matches[0])


def fetch_valid() -> tuple[pd.DataFrame, str]:
    """
    Load the valid skills output file.

    Returns:
        tuple: (DataFrame, file_name_without_extension)

    Raises:
        FileNotFoundError: If no valid skills file is found
    """
    return fetch_by_prefix("Valid Skills")


def fetch_invalid() -> tuple[pd.DataFrame, str]:
    """
    Load the invalid skills output file.

    Returns:
        tuple: (DataFrame, file_name_without_extension)

    Raises:
        FileNotFoundError: If no invalid skills file is found
    """
    return fetch_by_prefix("Invalid Skills")


def fetch_all_tagged() -> tuple[pd.DataFrame, str]:
    """
    Load the all tagged skills output file.

    Returns:
        tuple: (DataFrame, file_name_without_extension)

    Raises:
        FileNotFoundError: If no all tagged skills file is found
    """
    return fetch_by_prefix("All Tagged Skills")


def fetch_completed_output():
    """
    Load all completed output files (valid, invalid, and all tagged).

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

    Returns:
        dict: Checkpoint metadata containing 'round', 'progress', and 'sector'

    Raises:
        FileNotFoundError: If no checkpoint file is found
        RuntimeError: If multiple checkpoint files are found
    """
    pkl_files = list_files(checkpoint_path, "*.pkl")
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
    return bool(list_files(checkpoint_path, "*.pkl"))


def load_sfw_file() -> pd.DataFrame:
    """
    Load the SFW (Skills Framework) Excel file from input directory.

    Returns:
        pd.DataFrame: Loaded SFW data

    Raises:
        FileNotFoundError: If no file starting with 'SFW' is found
    """
    files = list_files(input_data_path, "*.xlsx")
    for fp in files:
        if Path(fp).name.startswith("SFW"):
            return load_excel(fp)
    raise FileNotFoundError(f"No file starting with 'SFW' in {input_data_path}")


def load_sector_file(cols=None) -> pd.DataFrame:
    """
    Load the sector Excel file (non-SFW file) from input directory.

    Args:
        cols (list, optional): Specific columns to load. Defaults to None.

    Returns:
        pd.DataFrame: Loaded sector data

    Raises:
        FileNotFoundError: If no sector file is found
    """
    files = list_files(input_data_path, "*.xlsx")
    for fp in files:
        if not Path(fp).name.startswith("SFW"):
            return load_excel(fp, usecols=cols)
    raise FileNotFoundError(f"No sector file found in {input_data_path}")


def load_r1_invalid() -> pd.DataFrame:
    """
    Load the round 1 invalid skills CSV file from intermediate output directory.

    Returns:
        pd.DataFrame: Loaded R1 invalid data

    Raises:
        FileNotFoundError: If no R1 invalid file is found
    """
    files = list_files(intermediate_output_path, "*.csv")
    for fp in files:
        if "r1_invalid" in Path(fp).name:
            return load_csv(fp)
    raise FileNotFoundError(
        f"No file containing 'r1_invalid' in {intermediate_output_path}"
    )


def load_r1_valid():
    """
    Load the round 1 valid skills CSV file from intermediate output directory.

    Returns:
        pd.DataFrame: Loaded R1 valid data

    Raises:
        FileNotFoundError: If no R1 valid file is found
    """
    files = list_files(intermediate_output_path, "*.csv")
    for fp in files:
        if "r1_valid" in Path(fp).name:
            return load_csv(fp)
    raise FileNotFoundError(
        f"No file containing 'r1_valid' in {intermediate_output_path}"
    )
