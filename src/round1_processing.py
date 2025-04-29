from r1_utils import *
import pandas as pd
import numpy as np
import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from datetime import datetime
import pickle as pkl

timestamp = datetime.now().strftime("%Y%m%d_%H%M")

from config import *
import logging

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=f"round_1_{target_sector_alias}_{timestamp}.log",
    filemode="w"
)

import sys
from pathlib import Path

parent_dir = Path.cwd().parent
sys.path.append(str(parent_dir))

# Call openai client from utils.py
client = get_openai_client(api_key, base_url)

# Parameters
sfw_raw_data_path = sfw_file_path
sfw_raw_data_sheet = sfw_sheet_reference

course_raw_data_path = f"{input_data_path}/{target_sector_alias}_course_pl_tagging_cleaned.xlsx"
course_raw_data_sheet = target_sector_alias
course_raw_data_cols = course_data_columns
course_descr_cols = ["Course Reference Number","Course Title","About This Course","What You'll Learn"]

course_descr_data_path = course_raw_data_path

r1_raw_output_path = f"{r1_output_path}/{target_sector_alias}_pl_tagging_raw_{timestamp}.csv"
r1_valid_output_path = f"{r1_output_path}/{target_sector_alias}_r1_valid_skill_pl_{timestamp}.csv"
r1_invalid_output_path = f"{r1_output_path}/{target_sector_alias}_r1_invalid_skill_pl_{timestamp}.csv"
sector_irrelevant_skill_path = f"{r1_output_path}/{target_sector_alias}_irrelevant_skills.csv"

sfw_raw_data = pd.read_excel(sfw_raw_data_path, sheet_name=sfw_raw_data_sheet)

# SFw data for the specific sector
knowledge_df = sfw_raw_data[sfw_raw_data["Sector"].isin(target_sector)]
knowledge_df = knowledge_df.reset_index(drop=True)
knowledge_df["skill_lower"] = [x.lower().strip() for x in knowledge_df["TSC_CCS Title"]]
print(f"Knowledge DF has {knowledge_df.shape[0]} rows and {knowledge_df.shape[1]} columns.\n")

# Course-Skill Data
course_raw_data = pd.read_excel(course_raw_data_path, sheet_name=course_raw_data_sheet, usecols=course_raw_data_cols)
course_raw_data = course_raw_data.drop_duplicates(subset=["Course Reference Number","Skill Title"], keep="first")
course_raw_data = course_raw_data.dropna()
course_raw_data = course_raw_data.reset_index(drop=True)
course_raw_data["skill_lower"] = [x.lower().strip() for x in course_raw_data["Skill Title"]]
print(f"Course raw DF has {course_raw_data.shape[0]} rows and {course_raw_data.shape[1]} columns.\n")

# Pure course data
course_description_raw_data = pd.read_excel(course_descr_data_path, usecols=course_descr_cols)
course_description_raw_data = course_description_raw_data.dropna()
course_description_raw_data = course_description_raw_data.reset_index(drop=True)
print(f"There are {course_description_raw_data.shape[0]} unique courses.\n")

# Check if all skills extracted can be found in SFw
extracted_skill_list = [x for x in course_raw_data["skill_lower"].unique()]
sfw_skill_list = [x for x in knowledge_df["skill_lower"].unique()]
beyond_sector_skills= list(set(extracted_skill_list).difference(set(sfw_skill_list)))
print(f"There are {len(beyond_sector_skills)} out-of-sector skills found in courses.\n")

# Separate "In-Sector" and "Not-In-Sector" skills
course_raw_data["Sector Relevance"] = course_raw_data["skill_lower"].apply(lambda x: "Not in sector" if x in beyond_sector_skills else "In Sector")

# Save those "Not In Sector" for eyeballing
sector_irrelevant_skills = course_raw_data[course_raw_data["Sector Relevance"]=="Not in sector"]
sector_irrelevant_skills = sector_irrelevant_skills.reset_index(drop=True)
print(f"Saving sector irrelevant skills file. There are {sector_irrelevant_skills.shape[0]} sector-irrelevant skills.\n")
sector_irrelevant_skills.to_csv(sector_irrelevant_skill_path, index=False)

# Keep to only "In Sector" skills
merged_course_df = course_raw_data[course_raw_data["Sector Relevance"]=="In Sector"]
merged_course_df = merged_course_df.reset_index(drop=True)
print(f"There are {merged_course_df.shape[0]} course skills for round 1 PL mapping.\n")

# Run GenAI and save / read pickles
results = run_in_parallel(merged_course_df, knowledge_df, client)
pickle_file_path = f"{r1_output_path}/{target_sector_alias}_r1_output.pkl"
with open(pickle_file_path, "wb") as file:
    pkl.dump(results, file)

with open(pickle_file_path, "rb") as file:
    results = pkl.load(file)

# Create a new column with lower case skills
results["skill_lower"] = [x.lower().strip() for x in results["Skill Title"]]

# Merge the LLM output with the original course-skill file
merged_final_df = pd.merge(merged_course_df, results, on=["Course Reference Number", "skill_lower"], suffixes=('', '_y'))
merged_final_df.drop(columns=["Skill Title_y"], inplace=True)

# Convert pl to int and save raw output file
merged_final_df['proficiency_level'] = merged_final_df['proficiency_level'].fillna(0)
merged_final_df['proficiency_level'] = [int(x) for x in merged_final_df['proficiency_level']]
merged_final_df.to_csv(r1_raw_output_path, index=False)

# Sieve out pl == 0 cases
untagged_cases = merged_final_df[merged_final_df.proficiency_level==0]
tagged_cases = merged_final_df[merged_final_df.proficiency_level>0]
tagged_cases = tagged_cases.reset_index(drop=True)

# Compress the skill to pl mapping to a list for validation against sfw
sanity_check_df = tagged_cases.groupby(by="Skill Title")["proficiency_level"].agg(set).reset_index()

sfw_pl_compressed_df = knowledge_df.groupby(by="TSC_CCS Title")["Proficiency Level"].agg(set).reset_index()
sfw_pl_compressed_df["skill_lower"] = [x.lower().strip() for x in sfw_pl_compressed_df["TSC_CCS Title"]]

# Record all cases of violation, except those with expected pl == 0 output
pl_violation_dict_list = []

for skill, pl_list in zip(sanity_check_df['Skill Title'], sanity_check_df['proficiency_level']):
    valid_pl_list = list(sfw_pl_compressed_df[sfw_pl_compressed_df.skill_lower==skill.lower().strip()]["Proficiency Level"])[0]
    logging.info(f"For skill: {skill}, SFw valid PLs: {valid_pl_list}, AI tagged PLs: {pl_list}")
    
    pl_violation_list = [pl for pl in pl_list if pl not in valid_pl_list]
    if len(pl_violation_list) == 0:
        pass
    else:
        pl_violation_dict_list.append({
            "skill": skill,
            "invalid_pl": pl_violation_list
        })

# Dataframe of skills with pl that's not found in sfw
pl_violation_df = pd.DataFrame(pl_violation_dict_list).explode("invalid_pl")
pl_violation_df.reset_index(drop=True, inplace=True)
pl_violation_df["skill_lower"] = [x.lower().strip() for x in pl_violation_df["skill"]]
pl_violation_df

# Merge violation df and all non-0 cases to filter out skills with invalid pl
violation_filtered_df = pd.merge(tagged_cases, pl_violation_df, left_on=["skill_lower","proficiency_level"], right_on=["skill_lower","invalid_pl"], how="outer", suffixes=("","_y"))
violation_filtered_df.drop(columns=["skill"], inplace=True)
violation_filtered_df.fillna(value=int(9), inplace=True)
violation_filtered_df

# Separate the valid and invalid cases
violation_filtered_df["invalid_pl"] = [int(x) for x in violation_filtered_df["invalid_pl"]]
valid_cases = violation_filtered_df[violation_filtered_df.invalid_pl==9]
invalid_cases = violation_filtered_df[violation_filtered_df.invalid_pl<9]

# Concat the invalid cases & untagged cases for round 2 processing
invalid_cases = invalid_cases.drop(columns=["invalid_pl"])
for_r2_processing_df = pd.concat([untagged_cases, invalid_cases], ignore_index=True)
for_r2_processing_df

# Save respective files
for_r2_processing_df.to_csv(r1_invalid_output_path, index=False)
valid_cases.to_csv(r1_valid_output_path, index=False)

