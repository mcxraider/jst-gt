# src/services/validation/data_structure_validation.py

import pandas as pd
import re
from config import SFW_EXPECTED_COLUMNS, SECTOR_EXPECTED_COLUMNS
from exceptions.data_validation_exception import DataValidationError


def validate_sfw_data_structure(df: pd.DataFrame) -> bool:
    """
    Validate SFW file data structure and column requirements.
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
    Validate sector file data structure and column requirements, including Skill Title formats.
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
