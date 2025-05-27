import streamlit as st

# import utils app components
from utils.session_handler import *
from services.checkpoint.checkpoint_pipeline import load_checkpoint_page
from frontend.sidebar_page import demo_sidebar
from frontend.homepage import homepage
from frontend.results_page import results_page
from frontend.upload_page import upload_file_page


def main():
    configure_page()

    demo_sidebar()  # attach only when doing the demo

    # --- Initial Choice Stage ---
    if st.session_state.app_stage == "initial_choice":
        homepage()

    # --- Stage for Uploading/Configuring New Process ---
    elif st.session_state.app_stage == "uploading_new":
        upload_file_page()

    elif st.session_state.app_stage == "load_checkpoint":
        load_checkpoint_page()

    # --- Results Ready Stage ---
    elif st.session_state.app_stage == "results_ready":
        results_page()


if __name__ == "__main__":
    main()
