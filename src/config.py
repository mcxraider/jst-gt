import yaml

# === Load YAML Configuration Files ===
with open("./configs/config.yaml", "r") as f:
    config = yaml.safe_load(f)

with open("./configs/skill_rac_chart.yaml", "r") as f:
    skill_proficiency_level_details = yaml.safe_load(f)

# === Directory Paths ===
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

# === AWS S3 Configuration ===
USE_S3 = config["USE_S3"]
S3_BUCKET_NAME = config["s3_bucket_name"]
AWS_REGION = config["aws_region"]
