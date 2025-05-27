# services/checkpoint/checkpoint_pipeline.py

from services.db import async_write_output_to_s3, load_checkpoint_metadata
from services.llm_pipeline.combined_pipeline import handle_core_processing
from config import process_alias_mapping


def resume_from_checkpoint(st):
    """
    Handles business logic for resuming from checkpoint.
    - Loads checkpoint metadata
    - Maps selected process/sector
    - Runs core processing
    - Writes output to S3
    Returns: results (or None if failure)
    """
    ckpt_metadata = load_checkpoint_metadata()
    st.session_state.selected_process_alias = ckpt_metadata.get("sector")
    st.session_state.selected_process = process_alias_mapping[
        st.session_state.selected_process_alias
    ]

    caption = st.empty()
    target_sector = st.session_state.selected_process
    target_sector_alias = st.session_state.selected_process_alias

    results = handle_core_processing(caption, target_sector, target_sector_alias)

    if not results:
        return None

    async_write_output_to_s3(caption, results)
    return results
