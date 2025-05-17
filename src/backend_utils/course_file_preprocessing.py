import pandas as pd
from config import (
    target_sector,
    target_sector_alias,
    course_file_path,
    sheet_name,
    course_data_columns,
    api_key,
    base_url,
)
import sys
from pathlib import Path
from ast import literal_eval

from config import (
    target_sector,
    target_sector_alias,
    course_file_path,
    sheet_name,
    course_data_columns,
    api_key,
    base_url,
)
import sys
from pathlib import Path
from ast import literal_eval

parent_dir = Path.cwd().parent
sys.path.append(str(parent_dir))

# Reference config file for information
raw_file_path = course_file_path
sheet_name = sheet_name
sector_abbrev = target_sector_alias
cols_to_use = course_data_columns

raw_file = pd.read_excel(raw_file_path, sheet_name=sheet_name, usecols=cols_to_use)

uploaded_file_rows = raw_file.shape[0]
print(f"Course Training file uploaded has {uploaded_file_rows} rows.")

# dropna for raw file
raw_file.dropna(inplace=True)
post_dropna_file_rows = raw_file.shape[0]
empty_row_count = uploaded_file_rows - post_dropna_file_rows
print(f"There were {empty_row_count} empty rows removed.")

# Dedup for raw file
raw_file.drop_duplicates(
    subset=["Course Reference Number", "Skill Title"], keep="first", inplace=True
)
raw_file.reset_index(drop=True, inplace=True)
post_dedup_file_rows = raw_file.shape[0]
dup_row_count = post_dropna_file_rows - post_dedup_file_rows
print(f"There were {dup_row_count} duplicated rows removed.")

# Complex Format Issues in Uploaded File
user_input = input("Does the file need complex reformatting? Y/N")
if user_input == "Y":
    # Create course ref file
    crs_list = raw_file.copy()
    crs_list = crs_list[
        [
            "Course Reference Number",
            "Course Title",
            "About This Course",
            "What You'll Learn",
        ]
    ]
    crs_list.drop_duplicates(keep="first", inplace=True)

    # Loop through skill content for format issues
    to_tag_crs_ref_list = []
    to_tag_skill_list = []

    tp_tag_crs_ref_list = []
    tp_tag_skill_list = []

    for _, row in raw_file.iterrows():
        skill_content = row["Skill Title"]
        course_ref = row["Course Reference Number"]
        if skill_content.startswith("["):
            if len(literal_eval(skill_content)) > 0:
                to_tag_crs_ref_list.append(course_ref)
                to_tag_skill_list.append(skill_content)
            else:
                pass
        else:
            tagged_skill_raw = skill_content.split(", ", 1)[0]
            tp_tag_crs_ref_list.append(course_ref)
            tp_tag_skill_list.append(tagged_skill_raw)

    # Create separate files to track TGS skill PL tags and skills for LLM
    to_be_tagged_df = pd.DataFrame(
        data={
            "Course Reference Number": to_tag_crs_ref_list,
            "Skill Title": to_tag_skill_list,
        }
    )
    tp_tagged_df = pd.DataFrame(
        data={
            "Course Reference Number": tp_tag_crs_ref_list,
            "Skill Title Raw": tp_tag_skill_list,
        }
    )

    # Merge only skills for LLM with course ref file
    raw_file_clean = to_be_tagged_df.merge(
        crs_list, on="Course Reference Number", how="left"
    )

    # Convert string to list using ast.literal_eval
    raw_file_clean["Skill Title"] = raw_file_clean["Skill Title"].apply(literal_eval)
    # Explode the 'skills' column
    course_skill_df = raw_file_clean.explode("Skill Title").reset_index(drop=True)

else:
    course_skill_df = raw_file.copy()
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
