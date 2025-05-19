import streamlit as st
from utils.output_handler import *
from utils.upload_pipeline import *
from backend_utils.combined_pipeline_claude import handle_core_processing
from components.buttons import *
from utils.db import *


def load_checkpoint_pipeline():
    """Simulates the 'Load Checkpoint' pipeline, assumes checkpoint yields Pandas DFs."""

    load_checkpoint_ui()

    disabled = st.session_state.processing

    # Load Checkpoint Button
    if st.button("Load Checkpoint", use_container_width=True, disabled=disabled):
        st.session_state.processing = True  # Lock button

        with st.spinner("Resuming from checkpoint…"):
            caption = st.empty()
            results = handle_core_processing(caption)

        # handle early exit
        if not results:
            st.session_state.processing = False  # Unlock the button again
            handle_exit("exit_from_failed_checkpoint")
            return

        async_write_output_to_s3(caption, results)

        # 5) update state and rerun
        st.session_state.results = results
        st.session_state.csv_yes = True
        st.session_state.app_stage = "results_ready"
        st.session_state.processing = False  # Unlock button

        st.rerun()

    back_homepage_button()
