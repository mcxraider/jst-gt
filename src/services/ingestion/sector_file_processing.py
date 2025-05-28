import pandas as pd
from utils.validation_utils import (
    has_mixed_skill_title_formats,
    build_course_skill_dataframe,
)


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
    return False


def run_preprocessing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply preprocessing steps to the sector file.

    Args:
        df: The sector dataframe to preprocess

    Returns:
        pd.DataFrame: The preprocessed dataframe
    """
    df = build_course_skill_dataframe(df)
    return df
