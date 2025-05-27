import streamlit as st
from utils.output_handler import *
from services.db.db import *
from components.page_header import *
from config import PDF_URL
from utils.session_handler import init_session_state


def demo_sidebar():
    st.sidebar.header("Navigation")
    with st.sidebar:
        if st.button("🏠 Home", use_container_width=True):
            st.session_state.app_stage = "initial_choice"
            current_pkl_state = st.session_state.pkl_yes
            init_session_state()
            st.session_state.pkl_yes = current_pkl_state
            st.rerun()

        if st.session_state.csv_yes and st.session_state.results:
            if st.button("📊 Results", use_container_width=True):
                st.session_state.app_stage = "results_ready"
                st.rerun()

        # Add help section
        st.markdown("---")
        with st.expander("📝 How To Use"):
            st.markdown(
                """
                - Select a department from the dropdown  
                - Upload your input file or load from a previous checkpoint  
                - Click **Process Data** to start the pipeline  
                - Once complete, download the output files
                """,
                unsafe_allow_html=True,
            )
            st.markdown(
                f'<a href="{PDF_URL}" target="_blank" download style="display:inline-block;padding:8px 16px;background:#eee;border-radius:4px;text-decoration:none;font-weight:bold;">📄 Download user format guide and documentation (PDF)</a>',
                unsafe_allow_html=True,
            )

        with st.expander("ℹ️ Contact"):
            st.markdown(
                """

        **Need help?**  
        Please contact:

        - jerry_yang_from.tp@tech.gov.sg
            """
            )
