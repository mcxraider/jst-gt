# src/services/validation/file_content_validation.py

import os
import pandas as pd
from typing import Tuple, Optional
from exceptions.data_validation_exception import DataValidationError
from exceptions.file_validation_exception import FileValidationError

# from services.validation.schema_validation import validate_sfw_schema, validate_sector_schema


async def validate_file_non_empty(uploaded_file_object) -> bool:
    """
    Ensure the uploaded file is not empty.
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
