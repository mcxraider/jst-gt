import streamlit as st
from typing import Optional, Tuple, List, Any
import os


def configure_page():
    """Set Streamlit page configuration and title."""
    page_title = "Proficiency Skills Tagging Processor"
    st.set_page_config(page_title=page_title, layout="wide")
    st.title(page_title)


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
        ckpt_path = "../s3_bucket/s3_checkpoint/ckpt.pkl"
        st.session_state["pkl_yes"] = os.path.exists(ckpt_path)

    if "exit_halfway" not in st.session_state:
        st.session_state["exit_halfway"] = False
