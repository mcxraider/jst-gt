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

process_choices = [
    "HR (Human Resource)",
    "FS (Food Services)",
    "FS (Financial Services)",
]

process_alias_mapping = {
    "HR": ["Human Resource"],
    "FS": ["Food Services"],
    "FS": ["Financial Services"],
}

INPUT_VALIDATION_SECTOR_CONFIG = {
    "Human Resource": "HR",
    "Food Services": "FS",
    "Financial Services": "FS",
}

SFW_EXPECTED_COLUMNS = {
    "TSC_CCS_Type": "object",
    "TSC_CCS Code": "object",
    "Sector": "object",
    "TSC_CCS Category": "object",
    "TSC_CCS Title": "object",
    "TSC_CCS Description": "object",
    "Proficiency Level": "int64",
    "Proficiency Description": "object",
    "Knowledge / Ability Classification": "object",
    "Knowledge / Ability Items": "object",
}


SECTOR_EXPECTED_COLUMNS = {
    "Course Reference Number": "object",
    "Skill Title": "object",
    "Course Title": "object",
    "About This Course": "object",
    "What You'll Learn": "object",
}
