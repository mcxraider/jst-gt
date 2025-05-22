import sys
from pathlib import Path
from ast import literal_eval
import pandas as pd
from typing import Optional, Tuple, Any, Callable

from config import (
    target_sector_alias,
    course_file_path,
    sheet_name,
    course_data_columns,
    course_descr_cols,
)

# allow imports from parent directory
parent_dir = Path.cwd().parent
sys.path.append(str(parent_dir))

# --- Cleaning Steps ---


def drop_empty_and_dedup(
    df: pd.DataFrame,
    subset: list[str],
) -> pd.DataFrame:
    """
    1) Drop rows with any NA in-place.
    2) Drop duplicate rows based on `subset`, keeping first.
    Returns the cleaned DataFrame.
    """
    df = df.dropna().drop_duplicates(subset=subset, keep="first").reset_index(drop=True)
    return df


# --- Complex Formatting ---


def extract_complex_skills(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    From rows whose Skill Title starts with '[':
    • literal_eval the list, explode it into separate rows.
    • preserve Course Reference Number and course metadata.
    """
    # keep only the course metadata for merging later
    crs_list = df[course_descr_cols].drop_duplicates(
        subset=["Course Reference Number"], keep="first"
    )

    # build lists of (course_ref, raw_skill_list)
    tag_refs, tag_skills = [], []
    for _, row in df.iterrows():
        skill = row["Skill Title"]
        crs_ref = row["Course Reference Number"]
        if isinstance(skill, str) and skill.startswith("["):
            parsed = literal_eval(skill)
            if parsed:
                tag_refs.append(crs_ref)
                tag_skills.append(parsed)

    tagged_df = pd.DataFrame(
        {
            "Course Reference Number": tag_refs,
            "Skill Title": tag_skills,
        }
    )

    # explode the list-of-skills into individual rows
    exploded = (
        tagged_df.merge(crs_list, on="Course Reference Number", how="left")
        .explode("Skill Title")
        .reset_index(drop=True)
    )
    return exploded


# --- Column Renaming ---


def safe_rename_skill_column(
    df: pd.DataFrame,
    old: str = "Skills Title 2K",
    new: str = "Skill Title",
) -> pd.DataFrame:
    """
    Rename `old` column to `new` if it exists.
    """
    if old in df.columns:
        return df.rename(columns={old: new})
    return df


def build_course_skill_dataframe(
    df: pd.DataFrame,
    complex_format: bool = False,
) -> pd.DataFrame:
    """
    Orchestrates the full pipeline:
    1) Load raw data
    2) Drop empty rows & dedupe
    3) Optionally handle complex formatting
    4) Rename skill column if needed
    Returns the final DataFrame.
    """
    # 2) Clean
    df = drop_empty_and_dedup(
        df,
        subset=["Course Reference Number", "Skill Title"],
    )

    # 3) Complex formatting (if requested)
    if complex_format:
        df = extract_complex_skills(df)

    # 4) Ensure correct column name
    df = safe_rename_skill_column(df)

    return df
