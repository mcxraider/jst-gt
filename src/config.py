import yaml

# Load the config.yaml file
with open("./configs/config.yaml", "r") as fname:
    config = yaml.safe_load(fname)

with open("./configs/skill_rac_chart.yaml", "r") as fname:
    skill_proficiency_level_details = yaml.safe_load(fname)


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

PDF_URL = config["PDF_URL"]

USE_S3 = config["USE_S3"]
S3_BUCKET_NAME = config["s3_bucket_name"]
AWS_REGION = config["aws_region"]
