import streamlit as st

from services.db import async_write_output_to_s3, load_checkpoint_metadata
from frontend.checkpoint_section import load_checkpoint_ui
from services.llm_pipeline.combined_pipeline import handle_core_processing
from config import process_alias_mapping


def handle_exit():
    st.error(
        "Processing was stopped midway due to a connection issue. If you would like to continue, start over and load from the previous checkpoint!"
    )
    st.session_state.app_stage = "initial_choice"


def load_checkpoint_page():
    """
    Page component that handles resuming from a checkpoint.
    """
    load_checkpoint_ui()

    disabled = st.session_state.processing

    if st.button("Load Checkpoint", use_container_width=True, disabled=disabled):
        st.session_state.processing = True  # Lock button
        ckpt_metadata = load_checkpoint_metadata()

        st.session_state.selected_process_alias = ckpt_metadata.get("sector")
        st.session_state.selected_process = process_alias_mapping[
            st.session_state.selected_process_alias
        ]

        with st.spinner("Resuming from checkpoint…"):
            caption = st.empty()
            target_sector = st.session_state.selected_process
            target_sector_alias = st.session_state.selected_process_alias
            print(f"Resuming with sector alias: {target_sector_alias}")

            results = handle_core_processing(
                caption, target_sector, target_sector_alias
            )

        if not results:
            st.session_state.processing = False
            handle_exit()
            return

        async_write_output_to_s3(caption, results)

        st.session_state.results = results
        st.session_state.csv_yes = True
        st.session_state.app_stage = "results_ready"
        st.session_state.processing = False

        st.rerun()
