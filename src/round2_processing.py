# Imports
from openai import AzureOpenAI
import pandas as pd
import pickle as pkl
import json
import re
import hashlib
import threading
import concurrent.futures
from ast import *
import string
from datetime import datetime
import logging
import openai
from r2_utils import *
import sys
from pathlib import Path
from config import *

timestamp = datetime.now().strftime("%Y%m%d_%H%M")
parent_dir = Path.cwd().parent
sys.path.append(str(parent_dir))

# Parameters
target_sector = target_sector
target_sector_alias = target_sector_alias
input_file_timestamp = input_file_timestamp
round_1_invalid_output_path = f"{r1_output_path}/{target_sector_alias}_r1_invalid_skill_pl_{input_file_timestamp}.csv"  # Check for the timestamp on file version used
round_1_valid_output_path = f"{r1_output_path}/{target_sector_alias}_r1_valid_skill_pl_{input_file_timestamp}.csv"  # Check for the timestamp on file version used

sfw_raw_data_path = sfw_file_path
sfw_raw_data_sheet = sfw_sheet_reference

course_raw_data_path = round_1_invalid_output_path
course_raw_data_sheet = sheet_name
course_raw_data_cols = course_data_columns
course_descr_cols = [
    "Course Reference Number",
    "Course Title",
    "About This Course",
    "What You'll Learn",
]

course_descr_data_path = course_raw_data_path

checkpoint_file_path = (
    f"../log_files/{target_sector_alias}_arc_chart_ref_checkpoint.txt"
)
pickle_file_path = f"{r2_output_path}/{target_sector_alias}_gpt_output.pkl"

r2_raw_output_path = (
    f"{r2_output_path}/{target_sector_alias}_course_skill_pl_rac_raw.csv"
)
r2_valid_output_path = (
    f"{r2_output_path}/{target_sector_alias}_r2_valid_skill_pl_{timestamp}.csv"
)
r2_invalid_output_path = (
    f"{r2_output_path}/{target_sector_alias}_r2_invalid_skill_pl_{timestamp}.csv"
)
all_valid_output_path = (
    f"{r2_output_path}/{target_sector_alias}_all_valid_skill_pl_{timestamp}.csv"
)

# LLM Config
def get_gpt_completion(sys_msg, model="gpt-4o", temperature=0.1):
    client = openai.OpenAI(api_key=api_key, base_url=base_url)
    response = client.chat.completions.create(
        model=model,
        messages=sys_msg,
        response_format={"type": "json_object"},
        seed=6800,
        temperature=temperature,
    )
    completion_output = literal_eval(response.choices[0].message.content)
    return completion_output


sys_msg = [
    {
        "role": "user",
        "content": "What is the colour of the sky? Reply me in JSON format {'sky_colour': 'colour'}.",
    }
]

test_result = get_gpt_completion(sys_msg=sys_msg)
print(f"Test call on colour of the sky: {test_result}\n")

# Load in TSC RAC Chart
from skill_rac_chart import skill_proficiency_level_details

data = pd.read_csv(round_1_invalid_output_path, low_memory=False)
data["course_text"] = (
    data["Course Title"]
    + " |: "
    + data["About This Course"]
    + " | "
    + data["What You'll Learn"]
)
data["unique_text"] = data["course_text"] + data["Skill Title"]
data["unique_id"] = data["unique_text"].apply(lambda x: generate_hash(x))

target_df = data.copy()
print(
    f"The Course DF has {target_df.shape[0]} rows, with {target_df.unique_id.nunique()} unique courses."
)

# Preprocessing Sector Knowledge Base
# Read in skill-level data
sfw_raw_data = pd.read_excel(sfw_raw_data_path, sheet_name=sfw_raw_data_sheet)

# Filter skill-level data to only Precision Engineering
knowledge_df = sfw_raw_data[sfw_raw_data["Sector"].isin(target_sector)]
knowledge_df = knowledge_df.reset_index(drop=True)
knowledge_df["skill_lower"] = [x.lower().strip() for x in knowledge_df["TSC_CCS Title"]]
knowledge_df["Knowledge / Ability Items"] = knowledge_df[
    "Knowledge / Ability Items"
].fillna("")
knowledge_df.groupby(["skill_lower", "Proficiency Level"], as_index=False).agg(
    {"Knowledge / Ability Items": ", ".join}
)

# Preprocessing Courses
# Read Course information
course_raw_data = pd.read_csv(
    course_raw_data_path, usecols=course_raw_data_cols, low_memory=False
)

# Read Course description
course_description_raw_data = pd.read_csv(
    course_descr_data_path, usecols=course_descr_cols, low_memory=False
)

# Drop rows without information
course_description_raw_data = course_description_raw_data.dropna()
course_description_raw_data = course_description_raw_data.drop_duplicates(
    subset=["Course Reference Number"], keep="first"
)
course_description_raw_data = course_description_raw_data.reset_index(drop=True)

# Merge both course information with the same course reference number
merged_course_df = pd.merge(
    course_description_raw_data,
    course_raw_data[["Skill Title", "Course Reference Number"]],
    on="Course Reference Number",
    how="inner",
)

merged_course_df["course_text"] = (
    merged_course_df["Course Title"]
    + " |: "
    + merged_course_df["About This Course"]
    + " | "
    + merged_course_df["What You'll Learn"]
)  # + " | " + merged_course_df["What You'll Learn"]
merged_course_df["unique_text"] = (
    merged_course_df["course_text"] + merged_course_df["Skill Title"]
)
merged_course_df["unique_id"] = merged_course_df["unique_text"].apply(
    lambda x: generate_hash(x)
)
print(f"After DF merge, it has {merged_course_df.shape[0]} rows.")

# RAC Chart Reference
kb_dic = knowledge_df.groupby("skill_lower").apply(
    lambda x: x.groupby("Proficiency Level")["Knowledge / Ability Items"]
    .apply(", ".join)
    .reset_index()
    .to_dict(orient="records")
)

id_list = []
result_list = []

id_list, result_list = get_result(
    df=target_df,
    max_worker=20,
    id_list=id_list,
    result_list=result_list,
    kb_dic=kb_dic,
    skill_pl_reference_chart=skill_proficiency_level_details,
    checkpoint_filename=checkpoint_file_path,
)

print(f"Sample result: {result_list[0], len(id_list)}\n")

# Save it to pkl
with open(pickle_file_path, "wb") as file:
    pkl.dump((id_list, result_list), file)

# Load it back from pkl
with open(pickle_file_path, "rb") as file:
    id_list, result_list = pkl.load(file)

# Parse out the result in DF
result_df = pd.DataFrame(
    data={
        "unique_id": id_list,
        "proficiency_level": [x["proficiency"] for x in result_list],
        "reason": [x["reason"] for x in result_list],
        "confidence": [x["confidence"] for x in result_list],
    }
)

sub_result_df = result_df[result_df.unique_id.isin(list(target_df.unique_id))]
sub_result_df.rename(
    columns={
        "proficiency_level": "proficiency_level_rac_chart",
        "reason": "reason_rac_chart",
        "confidence": "confidence_rac_chart",
    },
    inplace=True,
)

final_df = data.merge(sub_result_df, how="left", on="unique_id")

print(
    f"Number of untagged skills after R2: {sub_result_df[sub_result_df.proficiency_level_rac_chart==0].shape[0]}"
)
print(f"Number of skills processed in R2: {sub_result_df.shape[0]}")
print(f"Total number of skills passed on from R1: {data.shape[0]}")

final_df.drop(columns=["course_text", "unique_text", "unique_id"], inplace=True)

# Convert pl to int and save raw output file
final_df["proficiency_level"] = [int(x) for x in final_df["proficiency_level"]]
final_df.to_csv(r2_raw_output_path, index=False)

# Sanity Check
# Sieve out pl == 0 cases
r2_untagged_cases = final_df[final_df.proficiency_level_rac_chart == 0]
r2_tagged_cases = final_df[final_df.proficiency_level_rac_chart > 0]
r2_tagged_cases = r2_tagged_cases.reset_index(drop=True)

# Compress the skill to pl mapping to a list for validation against sfw
sanity_check_df = (
    r2_tagged_cases.groupby(by="Skill Title")["proficiency_level_rac_chart"]
    .agg(set)
    .reset_index()
)

sfw_pl_compressed_df = (
    knowledge_df.groupby(by="TSC_CCS Title")["Proficiency Level"].agg(set).reset_index()
)
sfw_pl_compressed_df["skill_lower"] = [
    x.lower().strip() for x in sfw_pl_compressed_df["TSC_CCS Title"]
]

# Record all cases of violation, except those with expected pl == 0 output
pl_violation_dict_list = []

for skill, pl_list in zip(
    sanity_check_df["Skill Title"], sanity_check_df["proficiency_level_rac_chart"]
):
    valid_pl_list = list(
        sfw_pl_compressed_df[sfw_pl_compressed_df.skill_lower == skill.lower().strip()][
            "Proficiency Level"
        ]
    )[0]
    logging.info(
        f"For skill: {skill}, SFw valid PLs: {valid_pl_list}, AI tagged PLs: {pl_list}"
    )

    pl_violation_list = [pl for pl in pl_list if pl not in valid_pl_list]
    if len(pl_violation_list) == 0:
        pass
    else:
        pl_violation_dict_list.append({"skill": skill, "invalid_pl": pl_violation_list})
print(f"Sample of invalid tags: {pl_violation_dict_list}")

# Dataframe of skills with pl that's not found in sfw
if len(pl_violation_dict_list) == 0:
    for_sanity_check_df = r2_invalid_cases = r2_untagged_cases.copy()
    r2_valid_cases = r2_tagged_cases
else:
    pl_violation_df = pd.DataFrame(pl_violation_dict_list).explode("invalid_pl")
    pl_violation_df.reset_index(drop=True, inplace=True)
    pl_violation_df["skill_lower"] = [
        x.lower().strip() for x in pl_violation_df["skill"]
    ]
    print(pl_violation_df.shape)

    # Merge violation df and all non-0 cases to filter out skills with invalid pl
    violation_filtered_df = pd.merge(
        r2_tagged_cases,
        pl_violation_df,
        left_on=["skill_lower", "proficiency_level_rac_chart"],
        right_on=["skill_lower", "invalid_pl"],
        how="outer",
        suffixes=("", "_y"),
    )
    violation_filtered_df.drop(columns=["skill"], inplace=True)
    violation_filtered_df.fillna(value=int(9), inplace=True)

    # Separate the valid and invalid cases
    violation_filtered_df["invalid_pl"] = [
        int(x) for x in violation_filtered_df["invalid_pl"]
    ]
    r2_valid_cases = violation_filtered_df[violation_filtered_df.invalid_pl == 9]
    r2_invalid_cases = violation_filtered_df[violation_filtered_df.invalid_pl < 9]

    # Concat the invalid cases & untagged cases for round 2 processing
    r2_invalid_cases = r2_invalid_cases.drop(columns=["invalid_pl"])
    for_sanity_check_df = pd.concat(
        [r2_untagged_cases, r2_invalid_cases], ignore_index=True
    )

# Merge r1 and r2 valid data
r1_valid_output = pd.read_csv(round_1_valid_output_path, low_memory=False)

r2_valid_output = r2_valid_cases.copy()
r2_valid_output["proficiency_level"] = r2_valid_output["proficiency_level_rac_chart"]
r2_valid_output["reason"] = r2_valid_output["reason_rac_chart"]
r2_valid_output["confidence"] = r2_valid_output["confidence_rac_chart"]
r2_valid_output = r2_valid_output.drop(
    columns=["proficiency_level_rac_chart", "reason_rac_chart", "confidence_rac_chart"]
)

all_valid_cases = pd.concat([r1_valid_output, r2_valid_output], ignore_index=True)
all_valid_cases = all_valid_cases.drop(columns=["invalid_pl"])
print(f"All valid skills PL tagging: {all_valid_cases.shape[0]}\n")

# Save respective files
for_sanity_check_df.reset_index(drop=True, inplace=True)
for_sanity_check_df.to_csv(r2_invalid_output_path, index=False)

r2_valid_cases.reset_index(drop=True, inplace=True)
r2_valid_cases.to_csv(r2_valid_output_path, index=False)

all_valid_cases.reset_index(drop=True, inplace=True)
all_valid_cases.to_csv(all_valid_output_path, index=False)

# Poor Data Quality Courses
original_crs_descr_df = pd.read_csv(course_descr_data_path)
merged_raw_course_df = pd.merge(
    original_crs_descr_df,
    course_raw_data[["Skill Title", "Course Reference Number"]],
    on="Course Reference Number",
    how="inner",
)
poor_dq_crs = merged_raw_course_df[
    ~merged_raw_course_df["Course Reference Number"].isin(
        list(final_df["Course Reference Number"])
    )
]
print(
    f"There are {poor_dq_crs['Course Reference Number'].nunique()} poor quality courses.\n"
)

missing_content_df = poor_dq_crs[poor_dq_crs["Course Title"].isnull()]
remaining_poor_dq_df = poor_dq_crs[
    ~poor_dq_crs["Course Reference Number"].isin(
        missing_content_df["Course Reference Number"]
    )
]

# Save poor quality course data
missing_content_df.to_csv(
    f"{r2_output_path}/{target_sector_alias}_missing_content_course.csv", index=False
)
remaining_poor_dq_df.to_csv(
    f"{r2_output_path}/{target_sector_alias}_poor_content_quality_course.csv",
    index=False,
)
