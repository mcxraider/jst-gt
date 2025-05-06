import streamlit as st
import pandas as pd
import numpy as np
from typing import Optional, Tuple, List, Any
import os
import time

# import utils file
from utils.input_handler import *
from utils.session_handler import *
from utils.output_handler import *
from utils.checkpoint_handler import *
from utils.db import *


def upload_new_pipeline():
    """Handles two file uploads and proceeds when both are valid."""
    st.subheader("1. Upload SFW Framework File")
    sfw_df, sfw_filename = upload_file(
        label="SFW Framework File", validator=validate_sfw_file_input
    )

    st.subheader("2. Upload Sector File")
    sector_df, sector_filename = upload_file(
        label="Sector File", validator=validate_sector_file_input
    )

    if sfw_df is not None and sector_df is not None:
        st.subheader("4. Start Processing")
        if st.button("Process Uploaded Data"):
            with st.spinner("Processing..."):
                # insert data into s3 with the unique name
                insert_input_to_s3_sync(sfw_filename, sfw_df, sector_filename, sector_df)

                df1, df2, df3 = handle_core_processing(sfw_df, sector_df)
                st.session_state.results = (df1, df2, df3)
                st.session_state.csv_yes = True
                st.session_state.app_stage = "results_ready"
                st.rerun()
    else:
        st.info("Please upload and validate both files to continue.")

    if st.button("Back to Choices"):
        st.session_state.app_stage = "initial_choice"
        st.session_state.csv_yes = False
        st.session_state.results = None
        st.rerun()


def main():
    configure_page()
    init_session_state()

    # --- Simulation Control (Optional - For Demo) ---
    st.sidebar.header("Demo Controls")
    default_pkl_exists = os.path.exists("my_checkpoint.pkl")
    simulate_pkl = st.sidebar.checkbox(
        "Simulate Checkpoint Available (pkl_yes)", value=st.session_state.pkl_yes
    )
    st.session_state.pkl_yes = simulate_pkl
    simulate_csv = st.sidebar.checkbox(
        "Simulate CSV Processed (csv_yes)", value=st.session_state.csv_yes
    )
    st.session_state.csv_yes = simulate_csv
    # if user toggles csv_yes but no results, create placeholder DataFrames
    if st.session_state.csv_yes and st.session_state.results is None:
        st.session_state.results = (
            pd.DataFrame({"note": ["Demo CSV1"]}),
            pd.DataFrame({"note": ["Demo CSV2"]}),
            pd.DataFrame({"note": ["Demo CSV3"]}),
        )
    if st.sidebar.button("Reset App State"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()
    # --- End Simulation Control ---

    # --- Initial Choice Stage ---
    if st.session_state.app_stage == "initial_choice":
        st.header("Choose an action:")

        pkl_available = st.session_state.pkl_yes
        load_checkpoint_enabled = pkl_available and not st.session_state.csv_yes

        col1, col2 = st.columns(2)

        with col1:
            if st.button(
                "⬆️ Upload New File & Run process",
                key="upload_new",
                use_container_width=True,
            ):
                st.session_state.app_stage = "uploading_new"
                st.rerun()

        with col2:
            if st.button(
                "🔄 Load from Previous Checkpoint",
                key="load_checkpoint",
                disabled=not load_checkpoint_enabled,
                use_container_width=True,
            ):
                load_checkpoint_pipeline()

        if not load_checkpoint_enabled and pkl_available:
            already_done_processing_msg = "Your Previous Run Job has already been processed. Download them below or start a new session!"
            st.info(already_done_processing_msg)
        elif not pkl_available:
            st.info("Please upload the files for processing!")

        # Immediate download buttons if CSV processed
        if st.session_state.csv_yes:
            df1, df2, df3 = st.session_state.results
            st.subheader("Download Previously Processed CSVs")
            col4, col5, col6 = st.columns(3)
            with col4:
                download_dataframe_as_csv(df1, "Result CSV 1", "res1")
            with col5:
                download_dataframe_as_csv(df2, "Result CSV 2", "res2")
            with col6:
                download_dataframe_as_csv(df3, "Result CSV 3", "res3")

    # --- Stage for Uploading/Configuring New Process ---
    elif st.session_state.app_stage == "uploading_new":
        upload_new_pipeline()

    # --- Results Ready Stage ---
    elif st.session_state.app_stage == "results_ready":
        st.header("Results")
        if st.session_state.csv_yes and st.session_state.results:
            st.success("Processing complete. You can now download the results as CSV.")
            df1, df2, df3 = st.session_state.results

            st.subheader("Download Processed CSVs")
            col1, col2, col3 = st.columns(3)
            with col1:
                download_dataframe_as_csv(df1, "Result CSV 1", "res1")
            with col2:
                download_dataframe_as_csv(df2, "Result CSV 2", "res2")
            with col3:
                download_dataframe_as_csv(df3, "Result CSV 3", "res3")
        else:
            st.warning("No results available to download.")

        if st.button("↩️ Start Over"):
            current_pkl_state = st.session_state.pkl_yes
            for key in list(st.session_state.keys()):
                if key != "pkl_yes":
                    del st.session_state[key]
            init_session_state()
            st.session_state.pkl_yes = current_pkl_state
            st.session_state.app_stage = "initial_choice"
            st.rerun()


if __name__ == "__main__":
    main()
