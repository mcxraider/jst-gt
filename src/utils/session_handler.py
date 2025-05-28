from pathlib import Path
from config import output_path
import streamlit as st
from services.db import check_pkl_existence

# --- Configuration ---


def configure_page_settings() -> None:
    """
    Configure the Streamlit page layout and initial sidebar state.

    This function sets the layout of the Streamlit page to 'wide' and expands the sidebar by default.
    It should be called at the start of the app to ensure consistent UI settings.
    """
    st.set_page_config(
        layout="wide",
        initial_sidebar_state="expanded",
    )


# --- Session State Initialization ---
def init_session_state() -> None:
    """
    Initialize Streamlit session state variables if not already set.

    This function sets up default values for various session state variables used throughout the app,
    such as result tracking, app stage, process selection, and placeholders for dynamic UI elements.
    It also checks for the existence of output CSV files and checkpoints, updating session state accordingly.
    """
    # Defaults for generic result tracking
    default_none_keys = ("results", "error_msg")
    for key in default_none_keys:
        st.session_state.setdefault(key, None)

    # Defaults for app state tracking
    st.session_state.setdefault("app_stage", "initial_choice")
    st.session_state.setdefault("exit_halfway", False)
    st.session_state.setdefault("selected_process", ["None"])
    st.session_state.setdefault("selected_process_alias", "")
    st.session_state.setdefault("processing", False)

    # Placeholder for dynamic captions
    if "caption_placeholder" not in st.session_state:
        st.session_state.caption_placeholder = st.empty()

    # Check existence of output CSV files
    if "csv_yes" not in st.session_state:
        s3_output_path = Path(output_path)
        output_files = (
            list(s3_output_path.glob("*.csv")) if s3_output_path.exists() else []
        )
        st.session_state["csv_yes"] = len(output_files) == 3

    # Check for existing checkpoints
    st.session_state.setdefault("pkl_yes", check_pkl_existence())


# --- Entry Point ---


def configure_page() -> None:
    """
    Set up the Streamlit page and initialize session state.

    This function combines page configuration and session state initialization to prepare the app for use.
    It should be called as the main entry point for UI and state setup.
    """
    configure_page_settings()
    init_session_state()
