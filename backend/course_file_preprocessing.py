import pandas as pd
from config import (
    target_sector_alias,
    course_file_path,
    sheet_name,
    course_data_columns,
)
import sys
from pathlib import Path

parent_dir = Path.cwd().parent
sys.path.append(str(parent_dir))

# Reference config file for information
raw_file_path = course_file_path
sheet_name = sheet_name
sector_abbrev = target_sector_alias
cols_to_use = course_data_columns

raw_file = pd.read_excel(raw_file_path, sheet_name=sheet_name, usecols=cols_to_use)
course_skill_df = raw_file.copy()

try:
    course_skill_df.rename(columns={"Skills Title 2K": "Skill Title"}, inplace=True)
except Exception as e:
    print(f"No column transformation needed: {e}")
    pass

course_skill_df.to_excel(
    f"../input_data/knowledge_transfer/{sector_abbrev}_course_pl_tagging_cleaned.xlsx",
    sheet_name=sector_abbrev,
    index=False,
)
