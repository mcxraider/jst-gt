import pandas as pd
from typing import Union
import json


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
