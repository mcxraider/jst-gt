import streamlit as st


# import utils file
from controllers.upload_controller import *
from utils.session_handler import *
from utils.output_utils import *
from services.db import *

from services.llm_pipeline.combined_pipeline import handle_core_processing
from frontend.components.page_header.page_header import *
from frontend.checkpoint_page import handle_exit


def process_uploaded_files(
    sfw_df: pd.DataFrame,
    sfw_filename: str,
    sector_df: pd.DataFrame,
    sector_filename: str,
):
    """Render the process button, upload to S3, run core processing, and update state."""
    selected_sector_alias = st.session_state.selected_process_alias
    selected_sector = st.session_state.selected_process
    st.subheader(f"3. Start Processing for {selected_sector_alias} sector")
    disabled = st.session_state.processing  # this is initialised to False

    if st.button("Process Data", disabled=disabled):
        st.session_state.processing = True  # Lock processing button

        with st.spinner("Processing..."):
            caption = st.empty()

            # 1) Wipe DB before uploading new data
            wipe_db(caption)

            # 2) upload inputs
            async_write_input_to_s3(
                caption, sfw_filename, sfw_df, sector_filename, sector_df
            )

            # 3) core processing (may exit early)
            results = handle_core_processing(
                caption, selected_sector, selected_sector_alias
            )

            # 4) handle early exit
            if not results:
                st.session_state.processing = False  # Unlock the button again
                handle_exit()
                return

            # 5) upload outputs
            async_write_output_to_s3(caption, results)

            # 6) update state and rerun
            st.session_state.results = results
            st.session_state.csv_yes = True
            st.session_state.app_stage = "results_ready"
            st.session_state.processing = False  # Unlock the button again

        st.rerun()
