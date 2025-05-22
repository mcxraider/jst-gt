import streamlit as st
from utils.output_handler import *
from utils.db import *
from components.buttons import *
from components.page_header import *


def homepage():
    create_header()

    # Action selection section
    st.markdown(
        """
    <div class="css-card">
        <h3 style="margin-top: 0;">Choose an Action:</h3>
    """,
        unsafe_allow_html=True,
    )

    pkl_available = st.session_state.pkl_yes
    load_checkpoint_enabled = pkl_available and not st.session_state.csv_yes

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="primary-button">', unsafe_allow_html=True)
        if st.button(
            f"⬆️ Upload New File & Run New Process",
            key="upload_new",
            use_container_width=True,
        ):
            st.session_state.app_stage = "uploading_new"
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="secondary-button">', unsafe_allow_html=True)
        if st.button(
            "🔄 Load from Previous Checkpoint",
            key="load_checkpoint",
            disabled=not load_checkpoint_enabled,
            use_container_width=True,
        ):
            st.session_state.app_stage = "load_checkpoint"
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

    # Status message
    if not load_checkpoint_enabled and pkl_available:
        st.info(
            """✅ Your previous run has already been processed. 
            Download the results below or start a new session!
        """
        )
        with st.expander("📂 Preview & Download Results"):
            dfs = fetch_completed_output()
            view_download_csvs(dfs)
    elif not pkl_available:
        st.info(
            """
            ℹ️ Please upload the files for processing!
        """
        )
    elif pkl_available:
        st.error(
            """
Your previous run stopped midway. Please start a new run or resume from your previous checkpoint!
        """
        )
