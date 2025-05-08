import streamlit as st
import pandas as pd

from utils.output_handler import *
from utils.db import *


def demo_sidebar():
    st.sidebar.header("Demo Controls")
    simulate_pkl = st.sidebar.checkbox(
        "Simulate Checkpoint Available (pkl_yes)", value=st.session_state.pkl_yes
    )
    st.session_state.pkl_yes = simulate_pkl
    
    simulate_csv = st.sidebar.checkbox(
        "Simulate all 3 CSVs Processed (csv_yes)", value=st.session_state.csv_yes
    )
    st.session_state.csv_yes = simulate_csv
    
    exit_halfway = st.sidebar.checkbox(
        "Simulate user Exiting halfway (exit_halfway)", value=st.session_state.exit_halfway
    )
    st.session_state.exit_halfway = exit_halfway

    # if user toggles csv_yes but no results, create placeholder DataFrames
    if st.session_state.csv_yes and st.session_state.results is None:
        st.session_state.results = (
            pd.DataFrame({"note": ["Demo CSV1"]}),
            pd.DataFrame({"note": ["Demo CSV2"]}),
            pd.DataFrame({"note": ["Demo CSV3"]}),
        )


def homepage():
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
                st.session_state.app_stage = "load_checkpoint"
                st.rerun()

        if not load_checkpoint_enabled and pkl_available:
            already_done_processing_msg = "Your Previous Run Job has already been processed. Download them below or start a new session!"
            st.info(already_done_processing_msg)
        elif not pkl_available:
            st.info("Please upload the files for processing!")

        # Immediate download buttons if CSV processed
        if st.session_state.csv_yes:
            dfs = fetch_completed_output()
            view_download_csvs(dfs) 


def results_page():
        st.header("Results")
        if st.session_state.csv_yes and st.session_state.results:
            st.success("Processing complete. You can now download the results as CSV.")
            dfs = st.session_state.results
            view_download_csvs(dfs)
        else:
            st.warning("No results available to download.")

