import pandas as pd
from typing import Union, Optional
import json
from ast import literal_eval

from config import COURSE_DESCR_COLS, S3_BUCKET_NAME
from services.storage.s3_client import get_s3_client
import logging

logger = logging.getLogger(__name__)


def is_list_like_string(value: Union[str, None]) -> bool:
    """
    Check if a string value represents a JSON list/array.
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
    """Check that both uploads succeeded."""
    return sfw_df is not None and sector_df is not None


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


def test_s3_put_delete_object(content: str, key: str = "s3_test_object.txt"):
    """
    A simple test to write a string to S3 and then delete it.
    """
    if not content:
        return

    try:
        s3_client = get_s3_client()
        logger.info(f"S3_TEST: Writing content to s3://{S3_BUCKET_NAME}/{key}")
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType="text/plain",
            ServerSideEncryption="AES256",
        )
        logger.info(f"S3_TEST: Deleting s3://{S3_BUCKET_NAME}/{key}")
        s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=key)
        logger.info("S3_TEST: Put/Delete test completed successfully.")
    except Exception as e:
        logger.error(f"S3_TEST: Put/Delete test failed: {e}")


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


# equivalent to run processing in input_handler.py
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
