# src/services/validation/file_content_validation.py

"""
File content validation module for ensuring uploaded files meet basic content requirements.

This module provides functions to validate the content of uploaded files, such as checking
for empty files and basic file integrity. It serves as the first line of validation
before more specific schema and data validation checks are performed.
"""

import os
from typing import Tuple, Optional
from exceptions.data_validation_exception import DataValidationError
from exceptions.file_validation_exception import FileValidationError

# from services.validation.schema_validation import validate_sfw_schema, validate_sector_schema


async def validate_file_non_empty(uploaded_file_object) -> bool:
    """
    Validate that an uploaded file contains data by checking its size.
    
    This function performs a basic check to ensure the uploaded file is not empty
    by seeking to the end of the file and checking its size. This is typically
    the first validation step performed on uploaded files.
    
    Args:
        uploaded_file_object: The file object to validate, typically from Streamlit's
            file uploader or similar file upload mechanism.
    
    Returns:
        bool: True if the file contains data (non-zero size)
    
    Raises:
        DataValidationError: If the file is empty or if there's an error checking
            the file size. The error message will provide guidance to the user.
    """
    try:
        uploaded_file_object.seek(0, os.SEEK_END)
        size = uploaded_file_object.tell()
        uploaded_file_object.seek(0)
        if size == 0:
            raise DataValidationError(
                "Your file looks empty. Please check that your file contains some data and try uploading again."
            )
        return True
    except DataValidationError:
        raise
    except Exception as e:
        raise DataValidationError(
            f"Something went wrong while checking your file size. " f"Details: {str(e)}"
        )
