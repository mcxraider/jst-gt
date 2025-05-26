import pandas as pd
import os
import re
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
# This dictionary maps full sector names to their short codes.
# All short codes should be uppercase.
# This simulates the "sector_in_full" list of allowed sectors.


ALLOWED_FULL_SECTORS = list(INPUT_VALIDATION_SECTOR_CONFIG.keys())
ALLOWED_SHORT_SECTORS = list(
    INPUT_VALIDATION_SECTOR_CONFIG.values()
)  # These are already uppercase from SECTOR_CONFIG

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

    Args:
        filename: The uploaded filename

    Returns:
        str: The extracted full_sector name (e.g., "Human Resource")

    Raises:
        FileValidationError: If filename doesn't match expected pattern or sector is not allowed.
    """
    if not filename:
        raise FileValidationError("Filename is empty")

    name_without_ext = Path(filename).stem

    # Check if filename starts with SFW_
    if not name_without_ext.startswith("SFW_"):
        raise FileValidationError(
            f"SFW filename must start with 'SFW_' followed by the full sector name. "
            f"For example: 'SFW_Human Resource.xlsx' or 'SFW_Food Services.xlsx'. Your file: '{filename}'"
        )

    # Extract full_sector part
    # Example: "SFW_Human Resource" -> "Human Resource"
    full_sector = name_without_ext[len("SFW_") :]

    if not full_sector:
        raise FileValidationError(
            "Full sector name cannot be empty after 'SFW_'. "
            f"For example: 'SFW_Human Resource.xlsx'. Your file: '{filename}'"
        )

    # Check if the extracted full_sector is in the allowed list
    if full_sector not in ALLOWED_FULL_SECTORS:
        raise FileValidationError(
            f"The full sector '{full_sector}' in your filename '{filename}' is not recognized. "
            f"Allowed full sectors are: {', '.join(ALLOWED_FULL_SECTORS)}."
        )

    return full_sector


def validate_sector_filename(filename: str) -> Tuple[str, str]:
    """
    Validate sector file naming convention: {sector}_{full_sector}_sector_course_listing_curated.xlsx
    (e.g., HR_Human Resource_sector_course_listing_curated.xlsx)
    The short 'sector' code must be uppercase.

    Args:
        filename: The uploaded filename

    Returns:
        Tuple[str, str]: (short_sector_code, full_sector_name)
                         e.g., ("HR", "Human Resource")

    Raises:
        FileValidationError: If filename doesn't match expected pattern or sector parts are invalid/not allowed.
    """
    if not filename:
        raise FileValidationError("Filename is empty")

    name_without_ext = Path(filename).stem

    required_suffix = "_sector_course_listing_curated"
    if not name_without_ext.endswith(required_suffix):
        raise FileValidationError(
            f"Sector filename must end with '{required_suffix}.xlsx'. "
            f"For example: 'HR_Human Resource_sector_course_listing_curated.xlsx'. "
            f"Your file: '{filename}'"
        )

    prefix = name_without_ext[: -len(required_suffix)]

    # Split the prefix to get sector code and full_sector name
    # Example: "HR_Human Resource" -> ("HR", "Human Resource")
    # Example: "FS_Food Services" -> ("FS", "Food Services")
    parts = prefix.split("_", 1)
    if len(parts) != 2:
        raise FileValidationError(
            f"Sector filename prefix '{prefix}' must follow the pattern '[SECTOR_CODE]_[Full_Sector_Name]'. "
            f"For example: 'HR_Human Resource' part in 'HR_Human Resource_sector_course_listing_curated.xlsx'. "
            f"Your file: '{filename}'"
        )

    short_sector, full_sector = parts
    if not short_sector:
        raise FileValidationError(
            "Short sector code in filename cannot be empty. "
            f"Use format like 'HR_Human Resource_sector_course_listing_curated.xlsx'. Your file: '{filename}'"
        )
    if not full_sector:
        raise FileValidationError(
            "Full sector name in filename cannot be empty. "
            f"Use format like 'HR_Human Resource_sector_course_listing_curated.xlsx'. Your file: '{filename}'"
        )

    # Check if short_sector is uppercase
    if short_sector != short_sector.upper():
        raise FileValidationError(
            f"The short sector code '{short_sector}' in your filename must be in uppercase. "
            f"Please use '{short_sector.upper()}'. Example: '{short_sector.upper()}_{full_sector}_sector_course_listing_curated.xlsx'. "
            f"Your file: '{filename}'"
        )

    # Check if the extracted short_sector is in the allowed list
    if short_sector not in ALLOWED_SHORT_SECTORS:
        raise FileValidationError(
            f"The short sector code '{short_sector}' in your filename '{filename}' is not recognized. "
            f"Allowed short sector codes are: {', '.join(ALLOWED_SHORT_SECTORS)}."
        )

    # Verify that the full_sector matches the short_sector according to SECTOR_CONFIG
    expected_full_sector = None
    for fs, sc in INPUT_VALIDATION_SECTOR_CONFIG.items():
        if sc == short_sector:
            expected_full_sector = fs
            break

    if full_sector != expected_full_sector:
        raise FileValidationError(
            f"The full sector name '{full_sector}' in your filename '{filename}' does not match the expected full name '{expected_full_sector}' for the short code '{short_sector}'. "
            f"Please ensure consistency, e.g., '{short_sector}_{expected_full_sector}_sector_course_listing_curated.xlsx'."
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

    Args:
        uploaded_file_object: Streamlit uploaded file object (or any file-like object)
        expected_sheet_name: Expected name of the single sheet

    Returns:
        pd.ExcelFile: Excel file object for further processing

    Raises:
        FileValidationError: If sheet structure is invalid
    """
    try:
        # Reset file pointer
        uploaded_file_object.seek(0)

        # Read Excel file
        excel_file = pd.ExcelFile(uploaded_file_object)

        # Check number of sheets
        if len(excel_file.sheet_names) != 1:
            raise FileValidationError(
                f"Your Excel file must contain exactly one sheet. "
                f"Currently it has {len(excel_file.sheet_names)} sheets: {excel_file.sheet_names}. "
                f"Please ensure only the '{expected_sheet_name}' sheet is present."
            )

        # Check sheet name
        actual_sheet_name = excel_file.sheet_names[0]
        if actual_sheet_name != expected_sheet_name:
            raise FileValidationError(
                f"The sheet name must be '{expected_sheet_name}'. "
                f"Currently it's named '{actual_sheet_name}'. "
                f"Please rename your sheet to '{expected_sheet_name}'."
            )

        return excel_file

    except FileValidationError:  # Re-raise if it's already our custom error
        raise
    except Exception as e:  # Catch other potential errors during Excel parsing
        raise FileValidationError(
            f"Error reading or validating Excel file structure: {str(e)}"
        )


# ===============================
# Data Structure Validation Functions
# (These functions remain unchanged as per the request, but are included for completeness)
# ===============================


def validate_sfw_data_structure(df: pd.DataFrame) -> bool:
    """
    Validate SFW file data structure and column requirements.
    (Content unchanged from original)
    """
    missing_columns = set(SFW_EXPECTED_COLUMNS.keys()) - set(df.columns)
    if missing_columns:
        raise FileValidationError(
            f"SFW file is missing required columns: {', '.join(sorted(missing_columns))}. "
            f"Please ensure your file has all these columns: {', '.join(sorted(SFW_EXPECTED_COLUMNS.keys()))}"
        )
    extra_columns = set(df.columns) - set(SFW_EXPECTED_COLUMNS.keys())
    if extra_columns:
        raise FileValidationError(
            f"SFW file has unexpected columns: {', '.join(sorted(extra_columns))}. "
            f"Please remove these columns and keep only: {', '.join(sorted(SFW_EXPECTED_COLUMNS.keys()))}"
        )
    dtype_errors = []
    for col, expected_dtype in SFW_EXPECTED_COLUMNS.items():
        actual_dtype = str(df[col].dtype)
        if expected_dtype == "int64" and actual_dtype in ["float64", "int64"]:
            if (
                df[col].isna().any()
            ):  # Check for NaN only if it could be float due to NaN
                # Allow conversion if possible, but if NaNs exist in an int column, it's an issue.
                if (
                    actual_dtype == "float64"
                    and df[col].dropna().apply(lambda x: x.is_integer()).all()
                ):
                    # It's float because of NaNs, but all non-NaNs are integers.
                    # This might be acceptable if NaNs are handled later or not allowed.
                    # For now, strict check: if expected is int, no NaNs.
                    dtype_errors.append(
                        f"Column '{col}' (expected whole numbers) contains empty/non-numeric cells. "
                        f"Please fill all cells in this column with whole numbers."
                    )
                elif (
                    actual_dtype == "int64" and df[col].isna().any()
                ):  # Should not happen for int64 directly
                    dtype_errors.append(
                        f"Column '{col}' (expected whole numbers) has an unexpected issue. Please check its data."
                    )
            # If it's already int64 and no NaNs, it's fine.
            # If it's float64 but all are integers (no NaNs), it's also fine.
            # The problem is float64 due to NaNs when int64 is expected.
            continue  # Skip further dtype check for this column if it passed int64/float64 logic

        if actual_dtype != expected_dtype:
            if expected_dtype == "object":
                expected_type_desc = "text"
            elif expected_dtype == "int64":
                expected_type_desc = "whole numbers"
            else:
                expected_type_desc = expected_dtype
            dtype_errors.append(
                f"Column '{col}' should contain {expected_type_desc}, but seems to contain {actual_dtype} data."
            )
    if dtype_errors:
        raise FileValidationError(
            f"SFW file has data type issues: " + "; ".join(dtype_errors)
        )
    if df.empty:
        raise FileValidationError(
            "SFW file contains no data rows. Please add your data."
        )
    return True


def validate_sector_data_structure(df: pd.DataFrame) -> bool:
    """
    Validate sector file data structure and column requirements, including Skill Title formats.
    - Required columns must exist and match expected names.
    - No extra columns allowed.
    - Data types for each column must match expectations.
    - File must not be empty and critical columns must have data.
    - Skill Title column values must be either:
        * plain text without any brackets (e.g., "Excel")
        * a bracketed list string (e.g., "['Python', 'SQL']").
    """

    # 1) Check for missing or extra columns
    missing_columns = set(SECTOR_EXPECTED_COLUMNS.keys()) - set(df.columns)
    if missing_columns:
        raise FileValidationError(
            f"Sector file is missing required columns: {', '.join(sorted(missing_columns))}. "
            f"Please ensure your file has all these columns: {', '.join(sorted(SECTOR_EXPECTED_COLUMNS.keys()))}"
        )
    extra_columns = set(df.columns) - set(SECTOR_EXPECTED_COLUMNS.keys())
    if extra_columns:
        raise FileValidationError(
            f"Sector file has unexpected columns: {', '.join(sorted(extra_columns))}. "
            f"Please remove these columns and keep only: {', '.join(sorted(SECTOR_EXPECTED_COLUMNS.keys()))}"
        )

    # 2) Validate data types
    dtype_errors = []
    for col, expected_dtype in SECTOR_EXPECTED_COLUMNS.items():
        actual_dtype = str(df[col].dtype)
        if actual_dtype != expected_dtype:
            dtype_errors.append(
                f"Column '{col}' should contain text, but contains {actual_dtype}"
            )
    if dtype_errors:
        raise FileValidationError(
            f"Sector file has data type issues: " + "; ".join(dtype_errors)
        )

    # 3) Ensure file not empty and critical columns populated
    if df.empty:
        raise FileValidationError(
            "Sector file contains no data rows. Please add your data."
        )
    for col in ["Course Reference Number", "Course Title"]:
        if df[col].isna().all():
            raise FileValidationError(
                f"Column '{col}' in the sector file cannot be completely empty. Please ensure this column has data."
            )

    # 4) Validate Skill Title formats
    # Allow either plain text without brackets or a bracketed list string
    plain_pattern = re.compile(r"^[^\[\]]+$")
    list_pattern = re.compile(r"^\[.*\]$")

    invalid_mask = df["Skill Title"].apply(
        lambda x: not (
            isinstance(x, str) and (plain_pattern.match(x) or list_pattern.match(x))
        )
    )
    if invalid_mask.any():
        invalid_values = df.loc[invalid_mask, "Skill Title"].unique().tolist()
        # fix f-string quoting by using a plain string for the second sentence
        raise FileValidationError(
            f"Invalid Skill Title format for values: {invalid_values}. "
            "Each entry must be either plain text without brackets (e.g., 'Excel') or a list string (e.g., ['Python', 'SQL'])."
        )

    return True


# ===============================
# File Content Validation Functions
# ===============================


async def validate_file_non_empty(uploaded_file_object) -> bool:
    """
    Ensure the uploaded file is not empty.
    (Content largely unchanged, uses uploaded_file_object consistently)
    """
    try:
        uploaded_file_object.seek(0, os.SEEK_END)
        size = uploaded_file_object.tell()
        uploaded_file_object.seek(0)  # Reset pointer for subsequent operations

        if size == 0:
            raise FileValidationError(
                "The uploaded file is empty. Please upload a file with data."
            )
        return True
    except FileValidationError:  # Re-raise if it's already our custom error
        raise
    except Exception as e:
        raise FileValidationError(f"Error checking file size: {str(e)}")


async def validate_sfw_schema(uploaded_file_object) -> bool:
    """
    Validate SFW file:
    1. File naming convention: SFW_{Full_Sector_Name}.xlsx (e.g., SFW_Human Resource.xlsx)
    2. Sheet naming convention: SFW_{SECTOR_CODE} (e.g., SFW_HR)
    3. Data structure.
    Assumes `uploaded_file_object` has a `name` attribute for the filename
    and is a file-like object for reading.
    """
    if not hasattr(uploaded_file_object, "name"):
        raise FileValidationError(
            "Uploaded object must have a 'name' attribute (filename)."
        )
    if not hasattr(uploaded_file_object, "seek") or not hasattr(
        uploaded_file_object, "read"
    ):
        raise FileValidationError(
            "Uploaded object must be a readable file-like object."
        )

    try:
        # Validate filename and get full sector name
        # Example: "Human Resource" from "SFW_Human Resource.xlsx"
        full_sector_name_from_file = validate_sfw_filename(uploaded_file_object.name)

        # Determine the expected short sector code from the full sector name
        short_sector_code = INPUT_VALIDATION_SECTOR_CONFIG.get(
            full_sector_name_from_file
        )
        if (
            not short_sector_code
        ):  # Should be caught by validate_sfw_filename, but as a safeguard
            raise FileValidationError(
                f"Internal error: Could not map full sector '{full_sector_name_from_file}' to a short code."
            )

        # Define expected sheet name based on new criteria: SFW_{SECTOR_CODE}
        # Example: "SFW_HR"
        expected_sheet_name = (
            f"SFW_{short_sector_code}"  # short_sector_code is already uppercase
        )

        # Validate Excel structure (only for Excel files)
        file_ext = Path(uploaded_file_object.name).suffix.lower()
        if file_ext not in [".xlsx", ".xls", ".csv"]:
            raise FileValidationError(
                f"Unsupported file type: '{file_ext}'. Please upload .xlsx, .xls, or .csv files."
            )

        # Ensure file is not empty before trying to parse
        await validate_file_non_empty(
            uploaded_file_object
        )  # uploaded_file_object.seek(0) is done inside

        if file_ext in [".xlsx", ".xls"]:
            excel_file = validate_excel_sheet_structure(
                uploaded_file_object, expected_sheet_name
            )
            # Read the data to validate structure
            # uploaded_file_object.seek(0) is done by validate_excel_sheet_structure before pd.ExcelFile
            df = pd.read_excel(excel_file, sheet_name=expected_sheet_name)
        elif file_ext == ".csv":
            # For CSV, sheet name validation is not applicable in the same way.
            # We assume the CSV directly contains the SFW data.
            # If CSVs need a "sheet name equivalent" check, this part needs clarification.
            # For now, we'll bypass sheet check for CSV and read it.
            uploaded_file_object.seek(0)
            df = pd.read_csv(uploaded_file_object)
        else:  # Should have been caught earlier
            raise FileValidationError(f"Unexpected file type: {file_ext}")

        # Validate data structure
        validate_sfw_data_structure(df)

        # Reset file pointer after validation for any further use
        uploaded_file_object.seek(0)

        return True

    except FileValidationError as e:  # Catch specific validation errors
        # Optionally, re-wrap or just raise to provide context
        # raise FileValidationError(f"SFW file validation failed: {str(e)}")
        raise  # Re-raise the specific error
    except Exception as e:  # Catch any other unexpected errors
        # Log the full error for debugging if possible: logging.exception("Unexpected SFW validation error")
        raise FileValidationError(
            f"An unexpected error occurred during SFW file validation: {str(e)}"
        )


async def validate_sector_schema(uploaded_file_object) -> bool:
    """
    Validate Sector file:
    1. File naming convention: {SECTOR_CODE}_{Full_Sector_Name}_sector_course_listing_curated.xlsx
       (e.g., HR_Human Resource_sector_course_listing_curated.xlsx)
    2. Sheet naming convention: {SECTOR_CODE} (e.g., HR)
    3. Data structure.
    Assumes `uploaded_file_object` has a `name` attribute for the filename
    and is a file-like object for reading.
    """
    if not hasattr(uploaded_file_object, "name"):
        raise FileValidationError(
            "Uploaded object must have a 'name' attribute (filename)."
        )
    if not hasattr(uploaded_file_object, "seek") or not hasattr(
        uploaded_file_object, "read"
    ):
        raise FileValidationError(
            "Uploaded object must be a readable file-like object."
        )

    try:
        # Validate filename and get short sector code and full sector name
        # Example: ("HR", "Human Resource") from "HR_Human Resource_sector_course_listing_curated.xlsx"
        short_sector_code, _ = validate_sector_filename(uploaded_file_object.name)
        # full_sector_name_from_file is also returned but not directly used for sheet name here.

        # Define expected sheet name based on new criteria: {SECTOR_CODE}
        # Example: "HR"
        expected_sheet_name = (
            short_sector_code  # short_sector_code is already validated to be uppercase
        )

        # Validate file extension and non-emptiness
        file_ext = Path(uploaded_file_object.name).suffix.lower()
        if file_ext not in [
            ".xlsx",
            ".xls",
        ]:  # Sector files are typically Excel as per original logic
            raise FileValidationError(
                f"Unsupported file type for Sector file: '{file_ext}'. Please upload .xlsx or .xls files."
            )

        await validate_file_non_empty(uploaded_file_object)

        # Validate Excel structure (sheet name and count)
        excel_file = validate_excel_sheet_structure(
            uploaded_file_object, expected_sheet_name
        )

        # Read the data to validate structure
        # uploaded_file_object.seek(0) is done by validate_excel_sheet_structure
        df = pd.read_excel(excel_file, sheet_name=expected_sheet_name)

        # Validate data structure
        validate_sector_data_structure(df)

        # Reset file pointer after validation
        uploaded_file_object.seek(0)

        return True

    except FileValidationError as e:
        # raise FileValidationError(f"Sector file validation failed: {str(e)}")
        raise  # Re-raise the specific error
    except Exception as e:
        # Log the full error for debugging if possible: logging.exception("Unexpected Sector validation error")
        raise FileValidationError(
            f"An unexpected error occurred during Sector file validation: {str(e)}"
        )


# ===============================
# Utility Functions for String Checks
# (These functions remain unchanged as per the request, but are included for completeness)
# ===============================


def is_list_like_string(value: Union[str, None]) -> bool:
    """
    Check if a string value represents a JSON list/array.
    (Content unchanged from original)
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
    (Content unchanged from original)
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
