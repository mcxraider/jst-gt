import streamlit as st
from typing import Optional, Tuple, List, Any
import os
from pathlib import Path


def configure_page():
    """Set Streamlit page configuration and title."""
    st.set_page_config(
        layout="wide",
        initial_sidebar_state="expanded",
    )


def check_pkl_existence() -> bool:
    checkpoint_dir = Path("../s3_bucket/s3_checkpoint")
    if not checkpoint_dir.exists():
        return False
    return any(checkpoint_dir.glob("*.pkl"))


# store these variables in S3.
def init_session_state():
    """Initialize session state variables."""
    for key in ("results", "error_msg"):
        if key not in st.session_state:
            st.session_state[key] = None

    if "app_stage" not in st.session_state:
        st.session_state["app_stage"] = "initial_choice"

    if "csv_yes" not in st.session_state:
        output_path = "../s3_bucket/s3_output/"
        output_files = os.listdir(output_path)
        num_outputs = len(output_files)
        if num_outputs == 3:
            st.session_state["csv_yes"] = True
        else:
            st.session_state["csv_yes"] = False

    if "pkl_yes" not in st.session_state:
        st.session_state["pkl_yes"] = check_pkl_existence()

    if "exit_halfway" not in st.session_state:
        st.session_state["exit_halfway"] = False

    if "selected_process" not in st.session_state:
        st.session_state["selected_process"] = "None"

    if "caption_placeholder" not in st.session_state:
        st.session_state.caption_placeholder = st.empty()
