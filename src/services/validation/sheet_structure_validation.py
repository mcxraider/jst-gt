# src/services/validation/sheet_structure_validation.py

"""
Sheet structure validation module for validating Excel file structure.

This module provides functions to validate the structure of Excel files, ensuring
they meet specific requirements for sheet count and naming. It is used as part of
the validation pipeline for both SFW and Sector files.
"""

import pandas as pd
from exceptions.data_validation_exception import DataValidationError


def validate_excel_sheet_structure(
    uploaded_file_object, expected_sheet_name: str
) -> pd.ExcelFile:
    """
    Validate that an Excel file has exactly one sheet with the expected name.
    
    This function performs two key validations on an Excel file:
    1. Ensures the file contains exactly one sheet
    2. Verifies that the sheet name matches the expected name
    
    Args:
        uploaded_file_object: The uploaded Excel file object to validate
        expected_sheet_name (str): The name that the single sheet should have
    
    Returns:
        pd.ExcelFile: A pandas ExcelFile object if validation passes
    
    Raises:
        DataValidationError: If validation fails, with a descriptive error message
            explaining the issue. Common errors include:
            - Multiple sheets present
            - Sheet name doesn't match expected name
            - File cannot be read as Excel
    
    Note:
        The function resets the file pointer to the beginning before reading
        to ensure consistent behavior regardless of previous file operations.
    """
    try:
        uploaded_file_object.seek(0)
        excel_file = pd.ExcelFile(uploaded_file_object)
        if len(excel_file.sheet_names) != 1:
            raise DataValidationError(
                f"Your Excel file should have just one sheet named '{expected_sheet_name}'. "
                f"Right now, it has {len(excel_file.sheet_names)} sheets: {excel_file.sheet_names}. "
                f"Please remove extra sheets and try again."
            )

        actual_sheet_name = excel_file.sheet_names[0]
        if actual_sheet_name != expected_sheet_name:
            raise DataValidationError(
                f"Your sheet should be named '{expected_sheet_name}', but it's currently called '{actual_sheet_name}'. "
                f"Please rename your sheet and try uploading again."
            )

        return excel_file
    except DataValidationError:
        raise
    except Exception as e:
        raise DataValidationError(
            f"Something went wrong while checking your Excel file. "
            f"Details: {str(e)}"
        )
