# OpenAI APIs

api_key = "sk-"
base_url = "https://ai-api.analytics.gov.sg"

# — Directory paths
input_data_path = "../temp_input"
intermediate_output_path = "../s3_bucket/s3_intermediate"
output_path = "../s3_bucket/s3_output"
misc_output_path = "../s3_bucket/s3_misc_output"
checkpoint_path = "../s3_bucket/s3_checkpoint"

# — Sector parameters
target_sector = ["Human Resource"]  # can have multiple
target_sector_alias = "HR"

# — Master course file
input_sector_filename = "SSG.TGS-CA-012_Course_Skill_Mapping_20240421_v08.xlsx"
course_raw_data_path = f"{input_data_path}/{input_sector_filename}"
sheet_name = "Sheet1"
course_data_columns = [
    "Course Reference Number",
    "Course Title",
    "Skill Title",
    "About This Course",
    "What You'll Learn",
]

# — SFW (supplemental) raw data
input_sfw_filename = "SFw_HR_Listing_v01.xlsx"

sfw_raw_data_path = f"{input_data_path}/{input_sfw_filename}"
sfw_raw_data_sheet = "SFW_HR"

# — Course‐specific column lists
course_raw_data_sheet = target_sector_alias
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
