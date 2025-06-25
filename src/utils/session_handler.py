import streamlit as st
from services.db import check_pkl_existence, check_output_existence
import sys
from pathlib import Path

# Add src directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config import PAGE_TITLE, PAGE_ICON

# --- Configuration ---


def configure_page_settings() -> None:
    """
    Configure the Streamlit page layout and initial sidebar state.

    This function sets the layout of the Streamlit page to 'wide' and expands the sidebar by default.
    It should be called at the start of the app to ensure consistent UI settings.
    """
    # Resolve the page icon path relative to the project root
    project_root = Path(__file__).parent.parent.parent
    icon_path = project_root / PAGE_ICON.lstrip("../")

    # For Streamlit page_icon, we'll use an emoji as the primary approach
    # The actual logo will be displayed in the UI components
    page_icon = "⛵"  # SAIL-themed emoji

    st.set_page_config(
        layout="wide",
        initial_sidebar_state="expanded",
        page_title=PAGE_TITLE,
        page_icon=page_icon,
    )


# --- Session State Initialization ---
def init_session_state() -> None:
    """
    Initialize Streamlit session state variables if not already set.

    This function sets up default values for various session state variables used throughout the app,
    such as result tracking, app stage, process selection, and placeholders for dynamic UI elements.
    It also checks for the existence of output CSV files and checkpoints, updating session state accordingly.
    """
    # Defaults for authentication
    st.session_state.setdefault("authenticated", False)
    st.session_state.setdefault("user_info", None)
    st.session_state.setdefault("username", None)
    st.session_state.setdefault("session_id", None)
    st.session_state.setdefault("stored_email", None)

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

    # Defaults for AI consent tracking
    st.session_state.setdefault("ai_consent_given", False)
    st.session_state.setdefault("show_ai_dialog", False)
    st.session_state.setdefault("start_processing", False)

    # Placeholder for dynamic captions
    if "caption_placeholder" not in st.session_state:
        st.session_state.caption_placeholder = st.empty()

    # Check existence of output CSV files (only if authenticated)
    if st.session_state.get("authenticated", False):
        st.session_state.csv_yes = check_output_existence()
        st.session_state.pkl_yes = check_pkl_existence()
    else:
        # Set to False when not authenticated
        st.session_state.setdefault("csv_yes", False)
        st.session_state.setdefault("pkl_yes", False)


def configure_page() -> None:
    """
    Set up the Streamlit page and initialize session state.

    This function combines page configuration and session state initialization to prepare the app for use.
    It should be called as the main entry point for UI and state setup.
    """
    configure_page_settings()
    init_session_state()
