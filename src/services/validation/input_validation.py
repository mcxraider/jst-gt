from typing import Tuple, Optional
from services.validation.schema_validation import *


async def validate_sfw_file_input(uploaded) -> Tuple[bool, Optional[str]]:
    """
    Run SFW-specific validation checks and return detailed results.

    Returns:
        Tuple[bool, Optional[str]]: (is_valid, error_message)
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
    Run Sector file-specific validation checks and return detailed results.

    Returns:
        Tuple[bool, Optional[str]]: (is_valid, error_message)
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
