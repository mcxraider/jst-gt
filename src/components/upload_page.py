import streamlit as st
from typing import Optional, Tuple, Any


# import utils file
from utils.input_handler import *
from utils.session_handler import *
from utils.output_handler import *
from utils.db import *
from utils.upload_pipeline import *
from components.buttons import *
from components.page_header import *
from backend_utils.config import process_choices


def upload_file_page():
    create_header()
    # Department selection with dropdown
    st.markdown("<h3>Select a Sector:</h3>", unsafe_allow_html=True)

    # Create a styled dropdown for process selection
    selected_process = st.selectbox(
        "Select a process:",
        process_choices,
        key="process_choice",
        help="Pick which pipeline you want to run.",
    )

    # Update the session state with the selected process
    st.session_state.selected_process_alias = selected_process[:2]
    st.session_state.selected_process = [selected_process[4:-1]]
    # Step 1: SFW framework file
    sfw_df, sfw_filename = prompt_file_upload(
        step=1,
        label=f"{st.session_state.selected_process[0]} SFW Framework",
        validator=validate_sfw_file_input,
    )

    # Step 2: Sector file
    sector_df, sector_filename = prompt_file_upload(
        step=2,
        label=f"{st.session_state.selected_process[0]} File",
        validator=validate_sector_file_input,
    )

    # Step 3: Check uploads and process or warn
    if both_files_uploaded(sfw_df, sector_df):
        process_uploaded_files(sfw_df, sfw_filename, sector_df, sector_filename)
    else:
        st.info("Please upload and validate both files to continue.")
