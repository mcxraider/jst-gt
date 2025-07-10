# file: r1_utils.py
from openai import OpenAI
from threading import Lock, local
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import json
from datetime import datetime
import os
from models.prompt_templates import R1_SYSTEM_PROMPT

from dotenv import load_dotenv

load_dotenv()

timestamp = datetime.now().strftime("%Y%m%d_%H%M")

# Thread-local storage for OpenAI client
thread_local = local()


def get_openai_client():
    """
    Returns a thread-local OpenAI client instance, creating one if it doesn't exist.
    Raises a ValueError if the API key is not found.
    """
    if not hasattr(thread_local, "client"):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is not set.")
        # Create a new client for this thread and store it in thread_local.
        thread_local.client = OpenAI(
            api_key=api_key, base_url="https://litellm.govtext.gov.sg/"
        )
    return thread_local.client


def get_skill_info(skill_title: str, skill_df: pd.DataFrame) -> dict:
    """Function that filters for skill_title"""
    skill_info = skill_df[skill_df["TSC_CCS Title"] == skill_title]
    proficiency_dict = {}
    levels = skill_info["Proficiency Level"].unique()

    for level in levels:
        level_info = skill_info[skill_info["Proficiency Level"] == level]
        knowledge_items = (
            level_info[level_info["Knowledge / Ability Classification"] == "knowledge"][
                "Knowledge / Ability Items"
            ]
            .unique()
            .tolist()
        )
        ability_items = (
            level_info[level_info["Knowledge / Ability Classification"] == "ability"][
                "Knowledge / Ability Items"
            ]
            .unique()
            .tolist()
        )
        proficiency_description = level_info["Proficiency Description"].iloc[0]
        proficiency_dict[level] = {
            "knowledge": knowledge_items,
            "ability": ability_items,
            "proficiency_description": proficiency_description,
        }
    return proficiency_dict


def format_for_openai(proficiency_info: dict, setup: int) -> str:
    """
    Formats data for OpenAI prompt
    setup is a parameter used to control the use of SFw information in the classification process:
    1: Only use Proficiency Description + Knowledge Items
    2: Only use Proficiency Description + Ability Items
    3: Use Proficiency Description + Knowledge + Ability Items
    """
    formatted_data = ""
    for level, items in proficiency_info.items():
        formatted_data += f"Proficiency Level: {level}\n"
        formatted_data += (
            f"Proficiency Description: {items['proficiency_description']}\n"
        )
        if setup == 1 or setup == 3:
            formatted_data += "Knowledge Items:\n"
            for item in items["knowledge"]:
                formatted_data += f"- {item}\n"
        if setup == 2 or setup == 3:
            formatted_data += "Ability Items:\n"
            for item in items["ability"]:
                formatted_data += f"- {item}\n"
        formatted_data += "\n"
    return formatted_data


# Original get_proficiency_level function (commented out for testing)
def get_proficiency_level(
    skill_title: str,
    skill_info: dict,
    course_description: str,
    course_learning: str,
    course_title: str,
    setup: int,
    client: OpenAI,  # client is now a required argument
) -> str:
    """
    Function to call OpenAI API.
    """
    formatted_data = format_for_openai(skill_info, setup)
    sys_messages = [
        {"role": "system", "content": R1_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                f'Determine the appropriate proficency level for skill: "{skill_title}", '
                f"based on how it's taught in the following description of a course: {course_title}, "
                f"Course Description: {course_description} Course Learning Objectives: {course_learning}. "
                f"And how its proficiency levels are defined: {formatted_data}."
            ),
        },
    ]
    try:
        response = client.chat.completions.create(
            model="gpt-4o-prd-gcc2-lb",
            messages=sys_messages,
            response_format={"type": "json_object"},
            seed=6800,
            temperature=0.1,
        )
        completion_output = response.choices[0].message.content
    except Exception as e:
        # Added a more descriptive error message and return value
        print(f"[ERROR] OpenAI API call failed in get_proficiency_level: {e}")
        completion_output = ""

    if completion_output is None:
        return ""
    return completion_output


def process_row(row, skill_info_dict, knowledge_df, lock):
    skill_title = row["Skill Title"]
    course_title = row["Course Title"]
    course_description = row["About This Course"]
    course_learning = row["What You'll Learn"]

    # Get the thread-local client.
    thread_client = get_openai_client()

    with lock:
        if skill_title in skill_info_dict:
            proficiency_info = skill_info_dict[skill_title]
        else:
            proficiency_info = get_skill_info(skill_title, knowledge_df)
            skill_info_dict[skill_title] = proficiency_info

    proficiency_level_with_reason = get_proficiency_level(
        skill_title,
        proficiency_info,
        course_description,
        course_learning,
        course_title,
        3,
        thread_client,
    )

    try:
        res_dict = json.loads(proficiency_level_with_reason)
        res_dict["Skill Title"] = row["Skill Title"]
        res_dict["Course Reference Number"] = row["Course Reference Number"]
    except json.JSONDecodeError as e:
        print(
            f"[ERROR] Failed to parse LLM response for {row['Course Reference Number']}: {e}"
        )
        res_dict = {
            "proficiency_level": 0,
            "reason": f"Failed to parse LLM JSON: {e}",
            "confidence": "low",
            "Skill Title": row["Skill Title"],
            "Course Reference Number": row["Course Reference Number"],
        }
    return res_dict


def run_in_parallel(course_df, knowledge_df):
    """
    Executes the processing of each row in parallel using a ThreadPoolExecutor.
    """
    skill_info_dict = {}
    results = []
    lock = Lock()
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(
                process_row, row, skill_info_dict, knowledge_df, lock
            )  # client is no longer passed as an argument
            for _, row in course_df.iterrows()
        ]
        for future in futures:
            results.append(future.result())
    results_df = pd.DataFrame(results)
    return results_df
