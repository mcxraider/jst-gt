# src/services/validation/sheet_structure_validation.py

import pandas as pd
from exceptions.data_validation_exception import DataValidationError


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
