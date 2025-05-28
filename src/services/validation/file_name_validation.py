# src/services/validation/file_name_validation.py

"""
File name validation module for validating uploaded file naming conventions.

This module provides functions to validate the naming conventions of uploaded files,
ensuring they follow the required format for both SFW (Skills Framework) and Sector
files. It validates sector codes, full sector names, and overall file name structure.
"""

from pathlib import Path
import streamlit as st
from config import INPUT_VALIDATION_SECTOR_CONFIG
from exceptions.file_validation_exception import FileValidationError

ALLOWED_FULL_SECTORS = list(INPUT_VALIDATION_SECTOR_CONFIG.keys())
ALLOWED_SHORT_SECTORS = list(INPUT_VALIDATION_SECTOR_CONFIG.values())


def validate_sfw_filename(filename: str) -> str:
    """
    Validate that an SFW file name follows the required naming convention.
    
    This function validates that an SFW file name follows the format:
    SFW_{full_sector}.xlsx (e.g., SFW_Human Resource.xlsx)
    
    The validation includes:
    1. File name starts with 'SFW_'
    2. Sector name is present after 'SFW_'
    3. Sector name is in the allowed list
    4. Sector code matches the selected process
    
    Args:
        filename (str): The name of the uploaded file to validate
    
    Returns:
        str: The validated full sector name
    
    Raises:
        FileValidationError: If validation fails, with a descriptive error message
            explaining the issue. Common errors include:
            - Empty filename
            - Missing 'SFW_' prefix
            - Missing sector name
            - Invalid sector name
            - Sector code mismatch with selected process
    
    Note:
        The function uses the INPUT_VALIDATION_SECTOR_CONFIG to validate sector
        names and codes against the allowed list.
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


def validate_sector_filename(filename: str):
    """
    Validate that a Sector file name follows the required naming convention.
    
    This function validates that a Sector file name follows the format:
    {sector}_{full_sector}_sector_course_listing_curated.xlsx
    (e.g., HR_Human Resource_sector_course_listing_curated.xlsx)
    
    The validation includes:
    1. File name ends with '_sector_course_listing_curated.xlsx'
    2. Sector code is present and in uppercase
    3. Full sector name is present
    4. Sector code is in the allowed list
    5. Full sector name matches the expected name for the code
    
    Args:
        filename (str): The name of the uploaded file to validate
    
    Returns:
        tuple: (short_sector, full_sector) containing the validated sector code
            and full sector name
    
    Raises:
        FileValidationError: If validation fails, with a descriptive error message
            explaining the issue. Common errors include:
            - Empty filename
            - Missing required suffix
            - Invalid sector code format
            - Invalid sector name
            - Sector code/name mismatch
    
    Note:
        The function uses the INPUT_VALIDATION_SECTOR_CONFIG to validate sector
        codes and names against the allowed list and ensure they match correctly.
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
