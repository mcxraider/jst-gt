"""
Sector file preprocessing utilities for ingestion pipeline.
Provides checks and transformations for sector files prior to main processing.

This module includes functions to determine if a sector file requires
preprocessing and to apply necessary transformations to standardize its format.
"""

import pandas as pd
from utils.validation_utils import (
    has_mixed_skill_title_formats,
    build_course_skill_dataframe,
)


def check_sector_requires_preprocessing(df: pd.DataFrame) -> bool:
    """
    Determine if the sector file requires preprocessing based on Skill Title format.
    
    Checks if the 'Skill Title' column in the DataFrame contains a mix of string
    and list formats, which indicates the need for preprocessing.
    
    Args:
        df (pd.DataFrame): The sector DataFrame to check
    
    Returns:
        bool: True if preprocessing is required, False otherwise
    """
    # Check if there's a mix of string and list formats in Skill Title column
    if has_mixed_skill_title_formats(df):
        return True
    return False


def run_preprocessing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply preprocessing steps to standardize the sector file format.
    
    Transforms the input DataFrame using utility functions to ensure that
    the course-skill relationships are properly structured for downstream processing.
    
    Args:
        df (pd.DataFrame): The sector DataFrame to preprocess
    
    Returns:
        pd.DataFrame: The preprocessed DataFrame, ready for main pipeline
    """
    df = build_course_skill_dataframe(df)
    return df
