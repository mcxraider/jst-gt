# OpenAI APIs
api_key = "sk-" # Input your own
base_url = "https://ai-api.analytics.gov.sg"

# Sector Pointer
target_sector = ["Sea Transport"]
target_sector_alias = "SeaTransport"

# Course File
course_file_path = "../input_data/knowledge_transfer/Sea Transport Course Listing.xlsx"
sheet_name = "Sheet1"
course_data_columns = [
    "Course Reference Number",
    "Course Title",
    "Skill Title",
    "About This Course",
    "What You'll Learn"
]

# SFw File
sfw_file_path = "../input_data/knowledge_transfer/Skills-Framework-Dataset-2024.xlsx"
sfw_sheet_reference = "TSC_CCS_K&A"

# Interim Input Data Directory
input_data_path = "../input_data/knowledge_transfer"
r1_output_path = "../round_1_output"
r2_output_path = "../round_2_output"

# CHECK FOR R1 OUTPUT TIMESTAMP & UPDATE HERE BEFORE RUNNING R2!!!
input_file_timestamp = "20250228_0100"