# services/db/session_management.py
"""
Session management utilities for Streamlit application.
Handles database cleanup and session state management.
"""
import streamlit as st

from config import base_dir
from services.storage import delete_all


def wipe_db(caption):
    """
    Clean up database by deleting all files from previous runs.

    Args:
        caption: Streamlit caption object for status updates

    Note:
        Only performs cleanup if csv_yes or pkl_yes flags are set in session state.
        Resets the session state flags after cleanup.
    """
    caption.caption("[Status] Erasing data from previous run...")

    # Check if cleanup is needed
    if not (
        st.session_state.get("csv_yes", False) or st.session_state.get("pkl_yes", False)
    ):
        return

    # Perform cleanup
    delete_all(base_dir)

    # Reset session state flags
    st.session_state["csv_yes"] = False
    st.session_state["pkl_yes"] = False
