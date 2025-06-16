from openai import OpenAI
from threading import Lock
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import json
from datetime import datetime
import os
import random
from models.prompt_templates import R1_SYSTEM_PROMPT
# Add these imports for environment variables
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Generate timestamped log filename
timestamp = datetime.now().strftime("%Y%m%d_%H%M")


def get_openai_client():
    client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'),
                    base_url="https://ai-api.analytics.gov.sg")
    return client


def get_skill_info(skill_title: str, skill_df: pd.DataFrame) -> dict:
    """Function that filters for skill_title"""
    skill_info = skill_df[
        skill_df["TSC_CCS Title"] == skill_title
    ]  # extracts all dataframe with the skill title
    proficiency_dict = {}
    levels = skill_info[
        "Proficiency Level"
    ].unique()  # get the all proficiency levels of skill

    for level in levels:
        level_info = skill_info[
            skill_info["Proficiency Level"] == level
        ]  # extracts all dataframes with the proficiency level
        knowledge_items = (
            level_info[level_info["Knowledge / Ability Classification"] == "knowledge"][
                "Knowledge / Ability Items"
            ]
            .unique()
            .tolist()
        )  # extract all knowledge of proficiency into skill
        ability_items = (
            level_info[level_info["Knowledge / Ability Classification"] == "ability"][
                "Knowledge / Ability Items"
            ]
            .unique()
            .tolist()
        )  # extract all ability of proficiency into skill
        proficiency_description = level_info["Proficiency Description"].iloc[
            0
        ]  # extract proficiency description
        proficiency_dict[level] = {
            "knowledge": knowledge_items,
            "ability": ability_items,
            "proficiency_description": proficiency_description,
        }
    # print(f"get_skill_info: {skill_title}")
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

    for level, items in proficiency_info.items():  # format data for LLM parsing
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


def get_proficiency_level(
    skill_title: str,
    skill_info: dict,
    course_description: str,
    course_learning: str,
    course_title: str,
    setup: int,
    client=None,
) -> str:
    """
    Simulated GPT call: randomly returns one of five example proficiency-level predictions.
    Signature matches get_proficiency_level, so you can swap them out easily.
    """
    examples = [
        {
            "proficiency_level": 0,
            "reason": "Course doesn’t cover this skill explicitly.",
            "confidence": "low",
        },
        {
            "proficiency_level": 1,
            "reason": "Introduces foundational concepts only.",
            "confidence": "high",
        },
        {
            "proficiency_level": 2,
            "reason": "Includes guided exercises applying the skill.",
            "confidence": "medium",
        },
        {
            "proficiency_level": 3,
            "reason": "Covers advanced theory and case studies.",
            "confidence": "medium",
        },
        {
            "proficiency_level": 2,
            "reason": "Skill used in group project settings.",
            "confidence": "high",
        },
    ]
    # pick one at random and return as JSON string
    return json.dumps(random.choice(examples))


# def get_proficiency_level(skill_title: str, skill_info: dict, course_description: str, course_learning: str, course_title: str, setup: int, client, system_prompt: str) -> str:
#     """
#     Function to call OpenAI API. Setup is called based on the classification type (1,2,3)
#     """
#     formatted_data = format_for_openai(skill_info, setup)
#     sys_messages = [
#         {
#             "role": "system",
#             "content": R1_SYSTEM_PROMPT
#         },
#         {
#             "role": "user",
#             "content": (
#                 f'Determine the appropriate proficency level for skill: "{skill_title}", '
#                 f'based on how it\'s taught in the following description of a course: {course_title}, '
#                 f'Course Description: {course_description} Course Learning Objectives: {course_learning}. '
#                 f'And how its proficiency levels are defined: {formatted_data}.'
#             )
#         }
#     ]

#     try:
#         response = client.chat.completions.create(
#             model="gpt-4o",
#             messages=sys_messages,
#             response_format={"type": "json_object"},
#             seed=6800,
#             temperature=0.1
#         )
#         completion_output = response.choices[0].message.content
#     except Exception:
#         completion_output = ""

#     return completion_output


def process_row(row, skill_info_dict, knowledge_df, lock, client):
    bad_course_filepath = "./bad_course_list.txt"
    skill_title = row["Skill Title"]
    course_title = row["Course Title"]
    course_description = row["About This Course"]
    course_learning = row["What You'll Learn"]

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
        client,
    )
    if proficiency_level_with_reason == "":
        if not os.path.exists(bad_course_filepath):
            with open(bad_course_filepath, "w") as file:
                file.write(f"""{row["Course Reference Number"]}\n""")
        else:
            with open(bad_course_filepath, "a") as fi:
                fi.write(f"""{row["Course Reference Number"]}\n""")
        pass

    try:
        res_dict = json.loads(proficiency_level_with_reason)
        res_dict["Skill Title"] = row["Skill Title"]
        res_dict["Course Reference Number"] = row["Course Reference Number"]
    except:
        print("Error")
        res_dict = {}
        res_dict["Skill Title"] = row["Skill Title"]
        res_dict["Course Reference Number"] = row["Course Reference Number"]

    return res_dict


def run_in_parallel(course_df, knowledge_df, client):
    skill_info_dict = {}
    results = []
    lock = Lock()
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = [
            executor.submit(
                process_row, row, skill_info_dict, knowledge_df, lock, client
            )
            for _, row in course_df.iterrows()
        ]
        for future in futures:
            results.append(future.result())
    results_df = pd.DataFrame(results)
    return results_df
