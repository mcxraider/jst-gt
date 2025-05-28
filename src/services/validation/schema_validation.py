# src/services/validation/schemas.py

import pandas as pd
from pathlib import Path
from config import INPUT_VALIDATION_SECTOR_CONFIG
from exceptions.file_validation_exception import FileValidationError
from services.validation.file_name_validation import (
    validate_sfw_filename,
    validate_sector_filename,
)
from services.validation.sheet_structure_validation import (
    validate_excel_sheet_structure,
)
from services.validation.data_structure_validation import (
    validate_sfw_data_structure,
    validate_sector_data_structure,
)
from services.validation.file_content_validation import validate_file_non_empty


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
