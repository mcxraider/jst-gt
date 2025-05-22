import streamlit as st
import pandas as pd
import os
import asyncio
from typing import Optional, Tuple, Any, Callable
from pathlib import Path

# ===============================
# Core Validation Functions
# ===============================

class FileValidationError(Exception):
    """Custom exception for file validation errors."""
    pass


async def validate_file_non_empty(uploaded) -> bool:
    """Ensure the uploaded file is not empty."""
    try:
        uploaded.seek(0, os.SEEK_END)
        size = uploaded.tell()
        uploaded.seek(0)
        
        if size == 0:
            raise FileValidationError("File is empty")
        return True
    except Exception as e:
        if isinstance(e, FileValidationError):
            raise
        raise FileValidationError(f"Error checking file size: {str(e)}")


def validate_sfw_filename(filename: str) -> str:
    """
    Validate SFW file naming convention: SFW_{sector}
    
    Args:
        filename: The uploaded filename
        
    Returns:
        str: The extracted sector code
        
    Raises:
        FileValidationError: If filename doesn't match expected pattern
    """
    if not filename:
        raise FileValidationError("Filename is empty")
    
    # Remove file extension for validation
    name_without_ext = Path(filename).stem
    
    # Check if filename starts with SFW_
    if not name_without_ext.startswith("SFW_"):
        raise FileValidationError(
            f"SFW filename must start with 'SFW_'. Got: {name_without_ext}"
        )
    
    # Extract sector part
    parts = name_without_ext.split("_")
    if len(parts) != 2:
        raise FileValidationError(
            f"SFW filename must follow pattern 'SFW_{{sector}}'. Got: {name_without_ext}"
        )
    
    sector = parts[1]
    if not sector:
        raise FileValidationError("Sector code cannot be empty in SFW filename")
    
    return sector


def validate_sector_filename(filename: str) -> Tuple[str, str]:
    """
    Validate sector file naming convention: {sector}_{full_sector}_sector_course_listing_curated.xlsx
    
    Args:
        filename: The uploaded filename
        
    Returns:
        Tuple[str, str]: (sector_code, full_sector_name)
        
    Raises:
        FileValidationError: If filename doesn't match expected pattern
    """
    if not filename:
        raise FileValidationError("Filename is empty")
    
    # Remove file extension for validation
    name_without_ext = Path(filename).stem
    
    # Check if filename ends with required suffix
    required_suffix = "_sector_course_listing_curated"
    if not name_without_ext.endswith(required_suffix):
        raise FileValidationError(
            f"Sector filename must end with '{required_suffix}'. Got: {name_without_ext}"
        )
    
    # Remove the suffix to get the prefix part
    prefix = name_without_ext[:-len(required_suffix)]
    
    # Split the prefix to get sector and full_sector
    parts = prefix.split("_", 1)  # Split only on first underscore
    if len(parts) != 2:
        raise FileValidationError(
            f"Sector filename must follow pattern '{{sector}}_{{full_sector}}_sector_course_listing_curated.xlsx'. Got: {name_without_ext}"
        )
    
    sector, full_sector = parts
    if not sector:
        raise FileValidationError("Sector code cannot be empty in sector filename")
    if not full_sector:
        raise FileValidationError("Full sector name cannot be empty in sector filename")
    
    return sector, full_sector


def validate_excel_sheet_structure(uploaded, expected_sheet_name: str) -> pd.ExcelFile:
    """
    Validate Excel file has exactly one sheet with the expected name.
    
    Args:
        uploaded: Streamlit uploaded file object
        expected_sheet_name: Expected name of the single sheet
        
    Returns:
        pd.ExcelFile: Excel file object for further processing
        
    Raises:
        FileValidationError: If sheet structure is invalid
    """
    try:
        # Reset file pointer
        uploaded.seek(0)
        
        # Read Excel file
        excel_file = pd.ExcelFile(uploaded)
        
        # Check number of sheets
        if len(excel_file.sheet_names) != 1:
            raise FileValidationError(
                f"Excel file must contain exactly one sheet. Found {len(excel_file.sheet_names)} sheets: {excel_file.sheet_names}"
            )
        
        # Check sheet name
        actual_sheet_name = excel_file.sheet_names[0]
        if actual_sheet_name != expected_sheet_name:
            raise FileValidationError(
                f"Sheet name must be '{expected_sheet_name}'. Found: '{actual_sheet_name}'"
            )
        
        return excel_file
        
    except Exception as e:
        if isinstance(e, FileValidationError):
            raise
        raise FileValidationError(f"Error validating Excel structure: {str(e)}")


async def validate_sfw_schema(uploaded) -> bool:
    """Validate SFW file schema and naming conventions."""
    try:
        # Validate filename
        sector = validate_sfw_filename(uploaded.name)
        
        # Expected sheet name
        expected_sheet_name = f"SFW_{sector}"
        
        # Validate Excel structure (only for Excel files)
        file_ext = Path(uploaded.name).suffix.lower()
        if file_ext in ['.xlsx', '.xls']:
            validate_excel_sheet_structure(uploaded, expected_sheet_name)
        
        # Reset file pointer after validation
        uploaded.seek(0)
        
        return True
        
    except FileValidationError as e:
        raise FileValidationError(f"SFW file validation failed: {str(e)}")
    except Exception as e:
        raise FileValidationError(f"Unexpected error during SFW validation: {str(e)}")


async def validate_sector_schema(uploaded) -> bool:
    """Validate sector file schema and naming conventions."""
    try:
        # Validate filename
        sector, full_sector = validate_sector_filename(uploaded.name)
        
        # Expected sheet name is just the sector code
        expected_sheet_name = sector
        
        # Validate Excel structure
        validate_excel_sheet_structure(uploaded, expected_sheet_name)
        
        # Reset file pointer after validation
        uploaded.seek(0)
        
        return True
        
    except FileValidationError as e:
        raise FileValidationError(f"Sector file validation failed: {str(e)}")
    except Exception as e:
        raise FileValidationError(f"Unexpected error during sector validation: {str(e)}")