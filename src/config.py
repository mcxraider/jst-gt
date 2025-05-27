import yaml

# Load the config.yaml file
with open("./config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Accessing values
base_dir = config["base_dir"]
input_data_path = config["input_data_path"]
intermediate_output_path = config["intermediate_output_path"]
output_path = config["output_path"]
misc_output_path = config["misc_output_path"]
checkpoint_path = config["checkpoint_path"]

course_data_columns = config["course_data_columns"]
course_descr_cols = config["course_descr_cols"]
process_choices = config["process_choices"]

process_alias_mapping = config["process_alias_mapping"]
INPUT_VALIDATION_SECTOR_CONFIG = config["INPUT_VALIDATION_SECTOR_CONFIG"]
SFW_EXPECTED_COLUMNS = config["SFW_EXPECTED_COLUMNS"]
SECTOR_EXPECTED_COLUMNS = config["SECTOR_EXPECTED_COLUMNS"]

PDF_URL = config["PDF_URL"]
R2_SYSTEM_PROMPT = config["R2_SYSTEM_PROMPT"]
R1_SYSTEM_PROMPT = config["R1_SYSTEM_PROMPT"]

USE_S3 = config["USE_S3"]
S3_BUCKET_NAME =  config["s3_bucket_name"]
AWS_REGION = config["aws_region"]