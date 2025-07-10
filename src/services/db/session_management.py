# services/db/session_management.py
"""
Session management utilities for Streamlit application.
Handles database cleanup and session state management.
"""
import streamlit as st

from config import (
    BASE_DIR,
    INPUT_DATA_PATH,
    INTERMEDIATE_OUTPUT_PATH,
    OUTPUT_PATH,
    MISC_OUTPUT_PATH,
    CHECKPOINT_PATH,
)
from services.storage import delete_all
from services.db import check_pkl_existence


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

    # change this to check pkl existence
    if not check_pkl_existence():
        print("pkl file not found")
        return

    # Perform cleanup
    for path in [
        INPUT_DATA_PATH,
        INTERMEDIATE_OUTPUT_PATH,
        OUTPUT_PATH,
        MISC_OUTPUT_PATH,
        CHECKPOINT_PATH,
    ]:
        delete_all(path)

    # Reset session state flags
    st.session_state["csv_yes"] = False
    st.session_state["pkl_yes"] = False
