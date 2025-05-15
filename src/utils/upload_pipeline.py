import streamlit as st
from typing import Optional, Tuple, List, Any


# import utils file
from utils.input_handler import *
from utils.session_handler import *
from utils.output_handler import *
from utils.checkpoint_pipeline import *
from utils.db import *
from backend_utils.combined_pipeline import *
from components.buttons import *


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


def handle_exit(button_title):
    st.error(
        "Processing was stopped midway due to a connection issue. If you would like to continue, start over and load from the previous checkpoint!"
    )
    st.session_state.app_stage = "initial_choice"
    back_homepage_from_failed_run_button(button_title)


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
            # 1) upload inputs
            async_write_input_to_s3(sfw_filename, sfw_df, sector_filename, sector_df)

            # 2) core processing (may exit early)
            results = handle_core_processing()

            # 3) handle early exit
            if not results:
                handle_exit("exit_from_failed_upload")
                return

            # 4) upload outputs
            async_write_output_to_s3(results)

            # 5) update state and rerun
            st.session_state.results = results
            st.session_state.csv_yes = True
            st.session_state.app_stage = "results_ready"
        st.rerun()
        back_homepage_button()


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
