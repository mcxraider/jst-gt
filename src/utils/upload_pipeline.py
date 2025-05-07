import streamlit as st
from typing import Optional, Tuple, List, Any


# import utils file
from utils.input_handler import *
from utils.session_handler import *
from utils.output_handler import *
from utils.checkpoint_handler import *
from utils.db import *


def prompt_file_upload(
    step: int, label: str, validator: Callable[[Any], asyncio.Future]
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Display a numbered subheader and file uploader for a given label and validator.
    Returns the DataFrame and the original filename if successful.
    """
    st.subheader(f"{step}. Upload {label}")
    return upload_file(label=label, validator=validator)


def both_files_uploaded(
    sfw_df: Optional[pd.DataFrame], sector_df: Optional[pd.DataFrame]
) -> bool:
    """Check that both uploads succeeded."""
    return sfw_df is not None and sector_df is not None


def process_uploaded_files(
    sfw_df: pd.DataFrame,
    sfw_filename: str,
    sector_df: pd.DataFrame,
    sector_filename: str,
):
    """Render the process button, upload to S3, run core processing, and update state."""
    st.subheader("3. Start Processing")
    if st.button("Process Uploaded Data"):
        with st.spinner("Processing..."):
            insert_input_to_s3_sync(sfw_filename, sfw_df, sector_filename, sector_df)
            df1, df2, df3 = handle_core_processing(sfw_df, sector_df)
            st.session_state.results = (df1, df2, df3)
            st.session_state.csv_yes = True
            st.session_state.app_stage = "results_ready"
            st.rerun()


def back_to_initial_choices():
    """Display a back button to reset state and return to initial choice."""
    if st.button("Back to Choices"):
        st.session_state.app_stage = "initial_choice"
        st.session_state.csv_yes = False
        st.session_state.results = None
        st.rerun()


def upload_new_pipeline():
    """Handles two file uploads with distinct validation logic and proceeds when ready."""
    # Step 1: SFW framework file
    sfw_df, sfw_filename = prompt_file_upload(
        step=1, label="SFW Framework File", validator=validate_sfw_file_input
    )

    # Step 2: Sector file
    sector_df, sector_filename = prompt_file_upload(
        step=2, label="Sector File", validator=validate_sector_file_input
    )

    # Step 3: Check uploads and process or warn
    if both_files_uploaded(sfw_df, sector_df):
        process_uploaded_files(sfw_df, sfw_filename, sector_df, sector_filename)
    else:
        st.info("Please upload and validate both files to continue.")

    # Back button
    back_to_initial_choices()
