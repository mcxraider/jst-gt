import streamlit as st

# import utils file
from controllers.upload_controller import upload_sfw_file, upload_sector_file
from services.ingestion.upload_pipeline import process_uploaded_files
from utils.validation_utils import both_files_uploaded


def file_selector():
    sfw_df, sfw_filename = upload_sfw_file()

    # Upload and validate sector file immediately (with preprocessing if needed)
    sector_df, sector_filename = upload_sector_file()

    # Step 3: Check uploads and process or warn
    if both_files_uploaded(sfw_df, sector_df):
        process_uploaded_files(sfw_df, sfw_filename, sector_df, sector_filename)
    else:
        st.info("Please upload and validate both files to continue.")
