import yaml
import os

from dotenv import load_dotenv, find_dotenv

dotenv_path = find_dotenv(".env.default", usecwd=True) or ".env.default"
load_dotenv(dotenv_path, override=True)

# === Load YAML Configuration Files ===
with open("./configs/config.yaml", "r") as f:
    config = yaml.safe_load(f)

with open("./configs/skill_rac_chart.yaml", "r") as f:
    skill_proficiency_level_details = yaml.safe_load(f)

# === AWS S3 Configuration ===
USE_S3 = config.get("USE_S3", False)
S3_BUCKET_NAME = "t-gen-stg-ssg-test-s3"  # Hardcoded for now
AWS_REGION = os.environ.get("AWS_REGION", "")

# Validate S3 configuration if S3 is enabled
if USE_S3 and not S3_BUCKET_NAME:
    raise ValueError(
        "S3_BUCKET_NAME environment variable is required when USE_S3 is True. "
        "Please set S3_BUCKET_NAME in your environment or deployment configuration."
    )

# === Directory Paths: Choose S3 or Local ===
if USE_S3:

    def convert_to_s3_path(prefix):  # Helper function for S3 URLs
        return f"s3://{S3_BUCKET_NAME}/{prefix}"

    INPUT_DATA_PATH = convert_to_s3_path(config["s3_input_prefix"])
    INTERMEDIATE_OUTPUT_PATH = convert_to_s3_path(config["s3_intermediate_prefix"])
    OUTPUT_PATH = convert_to_s3_path(config["s3_output_prefix"])
    MISC_OUTPUT_PATH = convert_to_s3_path(config["s3_misc_output_prefix"])
    CHECKPOINT_PATH = convert_to_s3_path(config["s3_checkpoint_prefix"])
    BASE_DIR = f"s3://{S3_BUCKET_NAME}/"
else:
    BASE_DIR = config["base_dir"]
    INPUT_DATA_PATH = config["input_data_path"]
    INTERMEDIATE_OUTPUT_PATH = config["intermediate_output_path"]
    OUTPUT_PATH = config["output_path"]
    MISC_OUTPUT_PATH = config["misc_output_path"]
    CHECKPOINT_PATH = config["checkpoint_path"]

# === Data Columns ===
COURSE_DATA_COLUMNS = config["course_data_columns"]
COURSE_DESCR_COLS = config["course_descr_cols"]

# === Process and Input Validation ===
PROCESS_CHOICES = config["process_choices"]
PROCESS_ALIAS_MAPPING = config["process_alias_mapping"]
INPUT_VALIDATION_SECTOR_CONFIG = config["input_validation_sector_config"]

# === External URLs ===
PDF_URL = config["PDF_URL"]

# === Authentication Configuration ===
AUTH_CONFIG = config.get("auth", {})
SESSION_TIMEOUT_HOURS = AUTH_CONFIG.get("session_timeout_hours", 2)
VALIDITY_MINUTES = AUTH_CONFIG.get("validity_minutes", 5)
APP_NAME = AUTH_CONFIG.get("app_name", "skill-proficiency-ai-tagger")
AUTH_DIR = AUTH_CONFIG.get("auth_dir", "../auth_data")
SESSIONS_DIR = AUTH_CONFIG.get("sessions_dir", "../auth_data/sessions")

# === UI Configuration ===
UI_CONFIG = config.get("ui", {})
PAGE_TITLE = UI_CONFIG.get("page_title", "SAIL - Skills Proficiency Tagging Portal")
PAGE_ICON = UI_CONFIG.get("page_icon", "../public/assets/images/sail_logo.png")
APP_NAME_DISPLAY = UI_CONFIG.get("app_name_display", "SAIL")
