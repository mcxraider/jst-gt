# frontend/checkpoint/checkpoint_page.py

import streamlit as st
from frontend.components.checkpoint_page.checkpoint_ui import load_checkpoint_ui
from services.checkpoint.resume_from_checkpoint import resume_from_checkpoint


def handle_exit():
    st.error(
        "Processing was stopped midway due to a connection issue. If you would like to continue, start over and load from the previous checkpoint!"
    )
    st.session_state.app_stage = "initial_choice"


def checkpoint_page():
    """
    Streamlit page: Handles UI and orchestration for resuming from a checkpoint.
    """
    load_checkpoint_ui()  # UI for loading checkpoint

    disabled = st.session_state.processing

    if st.button("Load Checkpoint", use_container_width=True, disabled=disabled):
        st.session_state.processing = True
        with st.spinner("Resuming from checkpoint…"):
            results = resume_from_checkpoint(st)  # Pass in session state or values

        if not results:
            st.session_state.processing = False
            handle_exit()
            return

        st.session_state.results = results
        st.session_state.csv_yes = True
        st.session_state.app_stage = "results_ready"
        st.session_state.processing = False
        st.rerun()
