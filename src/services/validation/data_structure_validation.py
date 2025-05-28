# src/services/validation/data_structure_validation.py

"""
Data structure validation module for validating the structure and content of DataFrame data.

This module provides functions to validate the structure and content of DataFrames
from both SFW (Skills Framework) and Sector files. It ensures that the data meets
all requirements including column presence, data types, and content formats.
"""

import pandas as pd
import re
from models.data_schema import SFW_EXPECTED_COLUMNS, SECTOR_EXPECTED_COLUMNS
from exceptions.data_validation_exception import DataValidationError


def validate_sfw_data_structure(df: pd.DataFrame) -> bool:
    """
    Validate the data structure and content of an SFW (Skills Framework) DataFrame.
    
    This function performs comprehensive validation of an SFW DataFrame, checking:
    1. Required columns are present
    2. No extra columns exist
    3. Column data types match expected types
    4. Numeric columns contain valid values
    5. DataFrame is not empty
    
    Args:
        df (pd.DataFrame): The DataFrame to validate, typically loaded from an SFW file
    
    Returns:
        bool: True if all validation checks pass
    
    Raises:
        DataValidationError: If any validation check fails, with a descriptive
            error message explaining the issue. Common errors include:
            - Missing required columns
            - Extra columns present
            - Incorrect data types
            - Invalid numeric values
            - Empty DataFrame
    
    Note:
        The function uses SFW_EXPECTED_COLUMNS from the data schema to validate
        column names and data types. Numeric columns are checked for both type
        and content validity.
    """
    missing_columns = set(SFW_EXPECTED_COLUMNS.keys()) - set(df.columns)
    if missing_columns:
        raise DataValidationError(
            f"Your file is missing these columns: {', '.join(sorted(missing_columns))}. "
            f"Please make sure your file includes all these columns: {', '.join(sorted(SFW_EXPECTED_COLUMNS.keys()))}."
        )
    extra_columns = set(df.columns) - set(SFW_EXPECTED_COLUMNS.keys())
    if extra_columns:
        raise DataValidationError(
            f"Your file has extra columns: {', '.join(sorted(extra_columns))}. "
            f"Please remove these and only keep: {', '.join(sorted(SFW_EXPECTED_COLUMNS.keys()))}."
        )
    dtype_errors = []
    for col, expected_dtype in SFW_EXPECTED_COLUMNS.items():
        actual_dtype = str(df[col].dtype)
        if expected_dtype == "int64" and actual_dtype in ["float64", "int64"]:
            if df[col].isna().any():
                if (
                    actual_dtype == "float64"
                    and df[col].dropna().apply(lambda x: x.is_integer()).all()
                ):
                    dtype_errors.append(
                        f"The column '{col}' should only contain whole numbers. "
                        f"Some cells seem empty or not numbers. Please check."
                    )
                elif actual_dtype == "int64" and df[col].isna().any():
                    dtype_errors.append(
                        f"The column '{col}' (whole numbers expected) has an unexpected issue. Please check this column."
                    )
            continue

        if actual_dtype != expected_dtype:
            if expected_dtype == "object":
                expected_type_desc = "text"
            elif expected_dtype == "int64":
                expected_type_desc = "whole numbers"
            else:
                expected_type_desc = expected_dtype
            dtype_errors.append(
                f"The column '{col}' should contain {expected_type_desc}, but seems to contain {actual_dtype} data."
            )
    if dtype_errors:
        raise DataValidationError(
            "Some columns in your file have unexpected data types: "
            + "; ".join(dtype_errors)
        )
    if df.empty:
        raise DataValidationError(
            "Your file doesn't have any data rows. Please add some data before uploading."
        )
    return True


def validate_sector_data_structure(df: pd.DataFrame) -> bool:
    """
    Validate the data structure and content of a Sector DataFrame.
    
    This function performs comprehensive validation of a Sector DataFrame, checking:
    1. Required columns are present
    2. No extra columns exist
    3. Column data types match expected types
    4. DataFrame is not empty
    5. Required columns have data
    6. Skill Title format is valid (plain text or list format)
    
    Args:
        df (pd.DataFrame): The DataFrame to validate, typically loaded from a Sector file
    
    Returns:
        bool: True if all validation checks pass
    
    Raises:
        DataValidationError: If any validation check fails, with a descriptive
            error message explaining the issue. Common errors include:
            - Missing required columns
            - Extra columns present
            - Incorrect data types
            - Empty DataFrame
            - Empty required columns
            - Invalid Skill Title format
    
    Note:
        The function uses SECTOR_EXPECTED_COLUMNS from the data schema to validate
        column names and data types. Skill Titles must be either plain text or
        in a list format like ['Python', 'SQL'].
    """
    missing_columns = set(SECTOR_EXPECTED_COLUMNS.keys()) - set(df.columns)
    if missing_columns:
        raise DataValidationError(
            f"Your file is missing these columns: {', '.join(sorted(missing_columns))}. "
            f"Please make sure your file includes all these columns: {', '.join(sorted(SECTOR_EXPECTED_COLUMNS.keys()))}."
        )
    extra_columns = set(df.columns) - set(SECTOR_EXPECTED_COLUMNS.keys())
    if extra_columns:
        raise DataValidationError(
            f"Your file has extra columns: {', '.join(sorted(extra_columns))}. "
            f"Please remove these and only keep: {', '.join(sorted(SECTOR_EXPECTED_COLUMNS.keys()))}."
        )

    dtype_errors = []
    for col, expected_dtype in SECTOR_EXPECTED_COLUMNS.items():
        actual_dtype = str(df[col].dtype)
        if actual_dtype != expected_dtype:
            dtype_errors.append(
                f"The column '{col}' should contain text, but currently contains {actual_dtype} data."
            )
    if dtype_errors:
        raise DataValidationError(
            "Some columns in your file have unexpected data types: "
            + "; ".join(dtype_errors)
        )

    if df.empty:
        raise DataValidationError(
            "Your file doesn't have any data rows. Please add some data before uploading."
        )
    for col in ["Course Reference Number", "Course Title"]:
        if df[col].isna().all():
            raise DataValidationError(
                f"The column '{col}' cannot be completely empty. Please make sure this column has data."
            )

    plain_pattern = re.compile(r"^[^\[\]]+$")
    list_pattern = re.compile(r"^\[.*\]$")

    invalid_mask = df["Skill Title"].apply(
        lambda x: not (
            pd.isna(x)
            or (
                isinstance(x, str) and (plain_pattern.match(x) or list_pattern.match(x))
            )
        )
    )

    if invalid_mask.any():
        invalid_values = df.loc[invalid_mask, "Skill Title"].dropna().unique().tolist()
        raise DataValidationError(
            f"Some skills in your file are in a format we don't recognize: {invalid_values}. "
            "Each skill should be typed as a simple name, like Excel, or as a list, like ['Python', 'SQL']. "
            "If there are no skills, you can leave it blank. "
            "Please correct these and try again."
        )

    return True
