import pandas as pd
from typing import Union, Optional
import json
from ast import literal_eval

from config import COURSE_DESCR_COLS


def is_list_like_string(value: Union[str, None]) -> bool:
    """
    Check if a string value represents a JSON list/array.
    
    Args:
        value (Union[str, None]): The string value to check
        
    Returns:
        bool: True if the string represents a valid JSON list/array, False otherwise
        
    Examples:
        >>> is_list_like_string("[1, 2, 3]")
        True
        >>> is_list_like_string("not a list")
        False
        >>> is_list_like_string(None)
        False
    """
    if pd.isna(value) or not isinstance(value, str):
        return False
    value = value.strip()
    if not (value.startswith("[") and value.endswith("]")):
        return False
    try:
        parsed = json.loads(value)
        return isinstance(parsed, list)
    except (json.JSONDecodeError, ValueError):
        return False


def has_mixed_skill_title_formats(df: pd.DataFrame) -> bool:
    """
    Check if the 'Skill Title' column contains a mix of regular strings and list-like strings.
    
    Args:
        df (pd.DataFrame): DataFrame containing a 'Skill Title' column
        
    Returns:
        bool: True if both regular strings and list-like strings are present, False otherwise
        
    Note:
        This function is used to detect inconsistent formatting in skill titles,
        where some entries are regular strings and others are JSON-like arrays.
    """
    if "Skill Title" not in df.columns:
        return False
    skill_title_series = df["Skill Title"].dropna()
    if skill_title_series.empty:
        return False
    list_like_count = 0
    regular_string_count = 0
    for value in skill_title_series:
        if is_list_like_string(value):
            list_like_count += 1
        elif isinstance(value, str) and value.strip():
            regular_string_count += 1
    return list_like_count > 0 and regular_string_count > 0


def both_files_uploaded(
    sfw_df: Optional[pd.DataFrame], sector_df: Optional[pd.DataFrame]
) -> bool:
    """
    Check that both required files have been successfully uploaded.
    
    Args:
        sfw_df (Optional[pd.DataFrame]): The SFW dataframe
        sector_df (Optional[pd.DataFrame]): The sector dataframe
        
    Returns:
        bool: True if both dataframes are not None, False otherwise
    """
    return sfw_df is not None and sector_df is not None


def drop_empty_and_dedup(
    df: pd.DataFrame,
    subset: list[str],
) -> pd.DataFrame:
    """
    Clean the dataframe by removing empty rows and duplicates.
    
    Args:
        df (pd.DataFrame): The input dataframe to clean
        subset (list[str]): List of column names to consider when dropping duplicates
        
    Returns:
        pd.DataFrame: Cleaned dataframe with:
            - No rows containing NA values
            - No duplicate rows based on the specified subset of columns
            - Reset index
    """
    df = df.dropna().drop_duplicates(subset=subset, keep="first").reset_index(drop=True)
    return df


# --- Complex Formatting ---


def extract_complex_skills(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Process rows with complex skill formatting (list-like strings) into separate rows.
    
    This function:
    1. Preserves course metadata
    2. Extracts skills from list-like strings
    3. Creates separate rows for each skill while maintaining course reference
    
    Args:
        df (pd.DataFrame): Input dataframe containing course and skill information
        
    Returns:
        pd.DataFrame: Processed dataframe with:
            - One row per skill from list-like strings
            - Preserved course metadata
            - Maintained course reference numbers
    """
    # keep only the course metadata for merging later
    crs_list = df[COURSE_DESCR_COLS].drop_duplicates(
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
    Safely rename a column in the dataframe if it exists.
    
    Args:
        df (pd.DataFrame): Input dataframe
        old (str, optional): Current column name. Defaults to "Skills Title 2K"
        new (str, optional): New column name. Defaults to "Skill Title"
        
    Returns:
        pd.DataFrame: DataFrame with renamed column if the old column existed,
                     otherwise returns the original dataframe unchanged
    """
    if old in df.columns:
        return df.rename(columns={old: new})
    return df


def build_course_skill_dataframe(
    df: pd.DataFrame,
    complex_format: bool = False,
) -> pd.DataFrame:
    """
    Orchestrate the full pipeline for processing course skill data.
    
    This function performs the following steps:
    1. Cleans the data by removing empty rows and duplicates
    2. Optionally handles complex formatting (list-like skill strings)
    3. Ensures consistent column naming
    
    Args:
        df (pd.DataFrame): Raw input dataframe
        complex_format (bool, optional): Whether to process complex formatting.
                                       Defaults to False.
        
    Returns:
        pd.DataFrame: Processed dataframe ready for further analysis
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
