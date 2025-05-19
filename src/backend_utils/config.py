import streamlit as st

# OpenAI APIs

api_key = "sk-"
base_url = "https://ai-api.analytics.gov.sg"

# — Directory paths
input_data_path = "../s3_bucket/s3_input"
intermediate_output_path = "../s3_bucket/s3_intermediate"
output_path = "../s3_bucket/s3_output"
misc_output_path = "../s3_bucket/s3_misc_output"
checkpoint_path = "../s3_bucket/s3_checkpoint"

course_data_columns = [
    "Course Reference Number",
    "Course Title",
    "Skill Title",
    "About This Course",
    "What You'll Learn",
]

# — Course‐specific column lists
course_raw_data_cols = [
    "Course Reference Number",
    "Skill Title",
    "Course Title",
    "About This Course",
    "What You'll Learn",
]
course_descr_cols = [
    "Course Reference Number",
    "Course Title",
    "About This Course",
    "What You'll Learn",
]
