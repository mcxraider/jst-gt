import streamlit as st
from utils.output_handler import *
from utils.upload_pipeline import *
from backend_utils.combined_pipeline import handle_core_processing
from backend_utils.config import process_alias_mapping, checkpoint_path
from components.buttons import *
from utils.db import *
import pickle


def load_checkpoint_sector():
    ckpt_dir = Path(checkpoint_path)
    pkl_files = list(ckpt_dir.glob("*.pkl"))

    if not pkl_files:
        raise FileNotFoundError("No checkpoint file found in the directory.")
    if len(pkl_files) > 1:
        raise RuntimeError("Multiple checkpoint files found. Expected only one.")

    ckpt_file = pkl_files[0]

    with open(ckpt_file, "rb") as f:
        data = pickle.load(f)

    metadata = {
        "round": data.get("round"),
        "progress": data.get("progress"),
        "sector": data.get("sector"),
    }

    return metadata


def load_checkpoint_pipeline():
    """Simulates the 'Load Checkpoint' pipeline, assumes checkpoint yields Pandas DFs."""

    load_checkpoint_ui()

    disabled = st.session_state.processing

    # Load Checkpoint Button
    if st.button("Load Checkpoint", use_container_width=True, disabled=disabled):
        st.session_state.processing = True  # Lock button
        ckpt_metadata = load_checkpoint_sector()

        st.session_state.selected_process_alias = ckpt_metadata.get("sector")
        st.session_state.selected_process = process_alias_mapping[
            st.session_state.selected_process_alias
        ]

        # gonna have to load the checkpoint data here and feed it into the handle core processing

        with st.spinner("Resuming from checkpoint…"):
            caption = st.empty()
            target_sector = st.session_state.selected_process
            target_sector_alias = st.session_state.selected_process_alias
            print(
                f"from the restart, the target sector alias is: {target_sector_alias}"
            )
            results = handle_core_processing(
                caption, target_sector, target_sector_alias
            )

        # handle early exit
        if not results:
            st.session_state.processing = False
            handle_exit()
            return

        async_write_output_to_s3(caption, results)

        # 5) update state and rerun
        st.session_state.results = results
        st.session_state.csv_yes = True
        st.session_state.app_stage = "results_ready"
        st.session_state.processing = False  # Unlock button

        st.rerun()