import pandas as pd
import os
import re
import streamlit as st
from pathlib import Path
import json
from typing import Tuple, Union
from backend_utils.config import (
    INPUT_VALIDATION_SECTOR_CONFIG,
    SFW_EXPECTED_COLUMNS,
    SECTOR_EXPECTED_COLUMNS,
)

# ===============================
# Sector Configuration
# ===============================
ALLOWED_FULL_SECTORS = list(INPUT_VALIDATION_SECTOR_CONFIG.keys())
ALLOWED_SHORT_SECTORS = list(INPUT_VALIDATION_SECTOR_CONFIG.values())

# ===============================
# Custom Exceptions
# ===============================


class FileValidationError(Exception):
    """Custom exception for file validation errors."""

    pass


# ===============================
# File Naming Validation Functions
# ===============================


def validate_sfw_filename(filename: str) -> str:
    """
    Validate SFW file naming convention: SFW_{full_sector}.xlsx
    (e.g., SFW_Human Resource.xlsx)
    """
    if not filename:
        raise FileValidationError(
            "Oops! It looks like you didn't upload a file. Please select a file to upload."
        )

    name_without_ext = Path(filename).stem
    if not name_without_ext.startswith("SFW_"):
        raise FileValidationError(
            f"Your file name should start with 'SFW_' and include the sector name. "
            f"For example: SFW_Human Resource.xlsx. "
            f"Your file is named: '{filename}'. "
            f"Please rename your file and try again."
        )

    full_sector = name_without_ext[len("SFW_") :]
    if not full_sector:
        raise FileValidationError(
            "The part after 'SFW_' in your file name seems to be missing. "
            f"For example: 'SFW_Human Resource.xlsx'. Your file: '{filename}'. "
            "Please check your file name and try again."
        )

    if full_sector not in ALLOWED_FULL_SECTORS:
        raise FileValidationError(
            f"The sector name '{full_sector}' in your file '{filename}' is not recognized. "
            f"Allowed sector names are: {', '.join(ALLOWED_FULL_SECTORS)}. "
            "Please check your file name and try again."
        )

    file_sector_alias = INPUT_VALIDATION_SECTOR_CONFIG.get(full_sector)

    if file_sector_alias != st.session_state.selected_process_alias:
        raise FileValidationError(
            f"The sector code in your file name '{filename}' (which maps to '{file_sector_alias}') "
            f"doesn't match your selected process '{st.session_state.selected_process_alias}'. "
            "Please make sure your file matches your selection and try again."
        )

    return full_sector


def validate_sector_filename(filename: str) -> Tuple[str, str]:
    """
    Validate sector file naming convention: {sector}_{full_sector}_sector_course_listing_curated.xlsx
    (e.g., HR_Human Resource_sector_course_listing_curated.xlsx)
    """
    if not filename:
        raise FileValidationError(
            "Oops! It looks like you didn't upload a file. Please select a file to upload."
        )

    name_without_ext = Path(filename).stem
    required_suffix = "_sector_course_listing_curated"
    if not name_without_ext.endswith(required_suffix):
        raise FileValidationError(
            f"Your file name should end with '{required_suffix}.xlsx'. "
            f"For example: HR_Human Resource_sector_course_listing_curated.xlsx. "
            f"Your file is named: '{filename}'. "
            f"Please rename your file and try again."
        )

    prefix = name_without_ext[: -len(required_suffix)]
    parts = prefix.split("_", 1)
    if len(parts) != 2:
        raise FileValidationError(
            f"Your file name should have the format '[SECTOR_CODE]_[Full_Sector_Name]'. "
            f"For example: HR_Human Resource_sector_course_listing_curated.xlsx. "
            f"Your file: '{filename}'. Please check and try again."
        )

    short_sector, full_sector = parts
    if not short_sector:
        raise FileValidationError(
            "The short sector code in your file name seems to be missing. "
            f"Use a name like HR_Human Resource_sector_course_listing_curated.xlsx. Your file: '{filename}'."
        )
    if not full_sector:
        raise FileValidationError(
            "The sector name in your file name seems to be missing. "
            f"Use a name like HR_Human Resource_sector_course_listing_curated.xlsx. Your file: '{filename}'."
        )

    if short_sector != short_sector.upper():
        raise FileValidationError(
            f"The sector code '{short_sector}' in your file name should be in capital letters. "
            f"For example: '{short_sector.upper()}_{full_sector}_sector_course_listing_curated.xlsx'. "
            f"Your file: '{filename}'. Please rename your file and try again."
        )

    if short_sector not in ALLOWED_SHORT_SECTORS:
        raise FileValidationError(
            f"The sector code '{short_sector}' in your file name '{filename}' isn't recognized. "
            f"Allowed sector codes are: {', '.join(ALLOWED_SHORT_SECTORS)}. Please check your file name and try again."
        )

    expected_full_sector = None
    for fs, sc in INPUT_VALIDATION_SECTOR_CONFIG.items():
        if sc == short_sector:
            expected_full_sector = fs
            break
    expected_fullname = st.session_state.selected_process[0]

    if full_sector != expected_fullname:
        raise FileValidationError(
            f"The sector name '{full_sector}' in your file '{filename}' doesn't match the expected name '{expected_fullname}' for the code '{short_sector}'. "
            f"Please rename your file like this: '{short_sector}_{expected_full_sector}_sector_course_listing_curated.xlsx'."
        )

    return short_sector, full_sector


# ===============================
# Excel Sheet Structure Validation
# ===============================


def validate_excel_sheet_structure(
    uploaded_file_object, expected_sheet_name: str
) -> pd.ExcelFile:
    """
    Validate Excel file has exactly one sheet with the expected name.
    """
    try:
        uploaded_file_object.seek(0)
        excel_file = pd.ExcelFile(uploaded_file_object)
        if len(excel_file.sheet_names) != 1:
            raise FileValidationError(
                f"Your Excel file should have just one sheet named '{expected_sheet_name}'. "
                f"Right now, it has {len(excel_file.sheet_names)} sheets: {excel_file.sheet_names}. "
                f"Please remove extra sheets and try again."
            )

        actual_sheet_name = excel_file.sheet_names[0]
        if actual_sheet_name != expected_sheet_name:
            raise FileValidationError(
                f"Your sheet should be named '{expected_sheet_name}', but it's currently called '{actual_sheet_name}'. "
                f"Please rename your sheet and try uploading again."
            )

        return excel_file

    except FileValidationError:
        raise
    except Exception as e:
        raise FileValidationError(
            f"Something went wrong while checking your Excel file. "
            f"Details: {str(e)}"
        )


# ===============================
# Data Structure Validation Functions
# ===============================


def validate_sfw_data_structure(df: pd.DataFrame) -> bool:
    """
    Validate SFW file data structure and column requirements.
    """
    missing_columns = set(SFW_EXPECTED_COLUMNS.keys()) - set(df.columns)
    if missing_columns:
        raise FileValidationError(
            f"Your file is missing these columns: {', '.join(sorted(missing_columns))}. "
            f"Please make sure your file includes all these columns: {', '.join(sorted(SFW_EXPECTED_COLUMNS.keys()))}."
        )
    extra_columns = set(df.columns) - set(SFW_EXPECTED_COLUMNS.keys())
    if extra_columns:
        raise FileValidationError(
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
        raise FileValidationError(
            "Some columns in your file have unexpected data types: "
            + "; ".join(dtype_errors)
        )
    if df.empty:
        raise FileValidationError(
            "Your file doesn't have any data rows. Please add some data before uploading."
        )
    return True


def validate_sector_data_structure(df: pd.DataFrame) -> bool:
    """
    Validate sector file data structure and column requirements, including Skill Title formats.
    """
    missing_columns = set(SECTOR_EXPECTED_COLUMNS.keys()) - set(df.columns)
    if missing_columns:
        raise FileValidationError(
            f"Your file is missing these columns: {', '.join(sorted(missing_columns))}. "
            f"Please make sure your file includes all these columns: {', '.join(sorted(SECTOR_EXPECTED_COLUMNS.keys()))}."
        )
    extra_columns = set(df.columns) - set(SECTOR_EXPECTED_COLUMNS.keys())
    if extra_columns:
        raise FileValidationError(
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
        raise FileValidationError(
            "Some columns in your file have unexpected data types: "
            + "; ".join(dtype_errors)
        )

    if df.empty:
        raise FileValidationError(
            "Your file doesn't have any data rows. Please add some data before uploading."
        )
    for col in ["Course Reference Number", "Course Title"]:
        if df[col].isna().all():
            raise FileValidationError(
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
        raise FileValidationError(
            f"Some skills in your file are in a format we don't recognize: {invalid_values}. "
            "Each skill should be typed as a simple name, like Excel, or as a list, like ['Python', 'SQL']. "
            "If there are no skills, you can leave it blank. "
            "Please correct these and try again."
        )

    return True


# ===============================
# File Content Validation Functions
# ===============================


async def validate_file_non_empty(uploaded_file_object) -> bool:
    """
    Ensure the uploaded file is not empty.
    """
    try:
        uploaded_file_object.seek(0, os.SEEK_END)
        size = uploaded_file_object.tell()
        uploaded_file_object.seek(0)
        if size == 0:
            raise FileValidationError(
                "Your file looks empty. Please check that your file contains some data and try uploading again."
            )
        return True
    except FileValidationError:
        raise
    except Exception as e:
        raise FileValidationError(
            f"Something went wrong while checking your file size. " f"Details: {str(e)}"
        )


async def validate_sfw_schema(uploaded_file_object) -> bool:
    """
    Validate SFW file:
    """
    if not hasattr(uploaded_file_object, "name"):
        raise FileValidationError(
            "Oops! Your uploaded file doesn't have a name. Please try uploading again."
        )
    if not hasattr(uploaded_file_object, "seek") or not hasattr(
        uploaded_file_object, "read"
    ):
        raise FileValidationError(
            "Your uploaded file can't be read. Please upload a standard file from your computer."
        )

    try:
        full_sector_name_from_file = validate_sfw_filename(uploaded_file_object.name)
        short_sector_code = INPUT_VALIDATION_SECTOR_CONFIG.get(
            full_sector_name_from_file
        )
        if not short_sector_code:
            raise FileValidationError(
                f"We couldn't figure out the sector code from '{full_sector_name_from_file}'. "
                "Please check your file name and try again."
            )

        expected_sheet_name = f"SFW_{short_sector_code}"

        file_ext = Path(uploaded_file_object.name).suffix.lower()
        if file_ext not in [".xlsx", ".xls", ".csv"]:
            raise FileValidationError(
                f"Sorry, we can't read files of type '{file_ext}'. Please upload an Excel (.xlsx, .xls) or CSV file."
            )

        await validate_file_non_empty(uploaded_file_object)

        if file_ext in [".xlsx", ".xls"]:
            excel_file = validate_excel_sheet_structure(
                uploaded_file_object, expected_sheet_name
            )
            df = pd.read_excel(excel_file, sheet_name=expected_sheet_name)
        elif file_ext == ".csv":
            uploaded_file_object.seek(0)
            df = pd.read_csv(uploaded_file_object)
        else:
            raise FileValidationError(f"Unexpected file type: {file_ext}")

        validate_sfw_data_structure(df)
        uploaded_file_object.seek(0)
        return True

    except FileValidationError as e:
        raise
    except Exception as e:
        raise FileValidationError(
            f"Something unexpected happened while checking your file. Details: {str(e)}"
        )


async def validate_sector_schema(uploaded_file_object) -> bool:
    """
    Validate Sector file:
    """
    if not hasattr(uploaded_file_object, "name"):
        raise FileValidationError(
            "Oops! Your uploaded file doesn't have a name. Please try uploading again."
        )
    if not hasattr(uploaded_file_object, "seek") or not hasattr(
        uploaded_file_object, "read"
    ):
        raise FileValidationError(
            "Your uploaded file can't be read. Please upload a standard file from your computer."
        )

    try:
        short_sector_code, _ = validate_sector_filename(uploaded_file_object.name)
        expected_sheet_name = short_sector_code

        file_ext = Path(uploaded_file_object.name).suffix.lower()
        if file_ext not in [".xlsx", ".xls"]:
            raise FileValidationError(
                f"Sorry, we can't read files of type '{file_ext}'. Please upload an Excel (.xlsx or .xls) file."
            )

        await validate_file_non_empty(uploaded_file_object)
        excel_file = validate_excel_sheet_structure(
            uploaded_file_object, expected_sheet_name
        )
        df = pd.read_excel(excel_file, sheet_name=expected_sheet_name)
        validate_sector_data_structure(df)
        uploaded_file_object.seek(0)
        return True

    except FileValidationError as e:
        raise
    except Exception as e:
        raise FileValidationError(
            f"Something unexpected happened while checking your file. Details: {str(e)}"
        )


# ===============================
# Utility Functions for String Checks
# ===============================


def is_list_like_string(value: Union[str, None]) -> bool:
    """
    Check if a string value represents a JSON list/array.
    """
    if pd.isna(value) or not isinstance(value, str):
        return False
    value = value.strip()
    if not (value.startswith("[") and value.endswith("]")):
        return False
    try:
        parsed = json.loads(value)
        return isinstance(parsed, list)
    except (json.JSONDecodeError, ValueError):
        return False


def has_mixed_skill_title_formats(df: pd.DataFrame) -> bool:
    """
    Check if the 'Skill Title' column contains a mix of regular strings and list-like strings.
    """
    if "Skill Title" not in df.columns:
        return False
    skill_title_series = df["Skill Title"].dropna()
    if skill_title_series.empty:
        return False
    list_like_count = 0
    regular_string_count = 0
    for value in skill_title_series:
        if is_list_like_string(value):
            list_like_count += 1
        elif isinstance(value, str) and value.strip():
            regular_string_count += 1
    return list_like_count > 0 and regular_string_count > 0
