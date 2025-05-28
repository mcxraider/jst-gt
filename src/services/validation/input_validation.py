"""
Input validation module for validating uploaded SFW and Sector files.

This module provides high-level validation functions that coordinate multiple validation
checks for different types of input files. It orchestrates the validation process by
running a series of checks and aggregating their results into a single validation outcome.
"""

from typing import Tuple, Optional
from services.validation.schema_validation import *


async def validate_sfw_file_input(uploaded) -> Tuple[bool, Optional[str]]:
    """
    Validate an uploaded SFW (Skills Framework) file through a series of checks.
    
    This function coordinates multiple validation checks specific to SFW files:
    1. File size validation to ensure the file is not empty
    2. SFW schema validation to ensure the file structure matches expected format
    
    Args:
        uploaded: The uploaded file object to validate, typically from Streamlit's
            file uploader or similar file upload mechanism.
    
    Returns:
        Tuple[bool, Optional[str]]: A tuple containing:
            - bool: True if all validation checks pass, False otherwise
            - Optional[str]: Error message describing validation failures, or None if valid
    
    Note:
        Each validation check is run independently, and all failures are collected
        into a single error message. This allows users to see all issues at once
        rather than fixing them one at a time.
    """
    validation_results = []
    error_messages = []

    # Run validation checks
    validation_checks = [
        ("File Size Check", validate_file_non_empty(uploaded)),
        ("SFW File Format Check", validate_sfw_schema(uploaded)),
    ]

    for check_name, check_coro in validation_checks:
        try:
            result = await check_coro
            validation_results.append(result)
        except FileValidationError as e:
            validation_results.append(False)
            error_messages.append(f"{check_name}: {str(e)}")
        except Exception as e:
            validation_results.append(False)
            error_messages.append(f"{check_name}: Unexpected error - {str(e)}")

    is_valid = all(validation_results)
    error_message = "; ".join(error_messages) if error_messages else None

    return is_valid, error_message


async def validate_sector_file_input(uploaded) -> Tuple[bool, Optional[str]]:
    """
    Validate an uploaded Sector file through a series of checks.
    
    This function coordinates multiple validation checks specific to Sector files:
    1. File size validation to ensure the file is not empty
    2. Sector file schema validation to ensure the file structure matches expected format
    
    Args:
        uploaded: The uploaded file object to validate, typically from Streamlit's
            file uploader or similar file upload mechanism.
    
    Returns:
        Tuple[bool, Optional[str]]: A tuple containing:
            - bool: True if all validation checks pass, False otherwise
            - Optional[str]: Error message describing validation failures, or None if valid
    
    Note:
        Each validation check is run independently, and all failures are collected
        into a single error message. This allows users to see all issues at once
        rather than fixing them one at a time.
    """
    validation_results = []
    error_messages = []

    # Run validation checks
    validation_checks = [
        ("File Size Check", validate_file_non_empty(uploaded)),
        ("Sector File Format Check", validate_sector_schema(uploaded)),
    ]

    for check_name, check_coro in validation_checks:
        try:
            result = await check_coro
            validation_results.append(result)
        except FileValidationError as e:
            validation_results.append(False)
            error_messages.append(f"{check_name}: {str(e)}")
        except Exception as e:
            validation_results.append(False)
            error_messages.append(f"{check_name}: Unexpected error - {str(e)}")

    is_valid = all(validation_results)
    error_message = "; ".join(error_messages) if error_messages else None

    return is_valid, error_message
