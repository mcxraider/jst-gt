# frontend/components/home_status_messages.py
import streamlit as st
from services.db import fetch_completed_output
from utils.output_utils import view_download_csvs


def home_status_messages(pkl_available, load_checkpoint_enabled):
    if not load_checkpoint_enabled and pkl_available:
        st.info(
            """✅ Your previous run has already been processed. 
            Download the results below or start a new session!
        """
        )
        with st.expander("📂 Preview & Download Results"):
            dfs = fetch_completed_output()
            view_download_csvs(dfs)
    elif not pkl_available:
        st.info(
            """
            ℹ️ Please upload the files for processing!
        """
        )
    elif pkl_available:
        st.error(
            """
Your previous run stopped midway. Please start a new run or resume from your previous checkpoint!
        """
        )
