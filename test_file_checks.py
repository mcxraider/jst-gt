#!/usr/bin/env python3
"""
Test script to verify file existence checks work correctly
from different working directories.
"""
import os
import sys
from pathlib import Path

# Add src to path
sys.path.append('src')

print(f"Current working directory: {os.getcwd()}")
print(f"Script location: {Path(__file__).parent}")

# Test from root directory
try:
    from src.services.db import check_pkl_existence, check_output_existence
    print(f"From root - CSV files exist: {check_output_existence()}")
    print(f"From root - PKL files exist: {check_pkl_existence()}")
except Exception as e:
    print(f"Error from root: {e}")

# Test from src directory
os.chdir('src')
print(f"\nChanged to src directory: {os.getcwd()}")

try:
    from services.db import check_pkl_existence, check_output_existence
    print(f"From src - CSV files exist: {check_output_existence()}")
    print(f"From src - PKL files exist: {check_pkl_existence()}")
except Exception as e:
    print(f"Error from src: {e}")
