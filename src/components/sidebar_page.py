import streamlit as st
from utils.output_handler import *
from utils.db import *
from components.buttons import *
from components.page_header import *


def demo_sidebar():
    st.sidebar.header("Demo Controls (for Beta)")
    with st.sidebar:
        simulate_pkl = st.sidebar.checkbox(
            "Simulate Checkpoint Available (pkl_yes)", value=st.session_state.pkl_yes
        )
        st.session_state.pkl_yes = simulate_pkl

        simulate_csv = st.sidebar.checkbox(
            "Simulate All 3 CSVs Processed (csv_yes)", value=st.session_state.csv_yes
        )
        st.session_state.csv_yes = simulate_csv

        exit_halfway = st.sidebar.checkbox(
            "Simulate User/system exit (exit_halfway)",
            value=st.session_state.exit_halfway,
        )
        st.session_state.exit_halfway = exit_halfway

        st.markdown("## Navigation")

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
                        """
            )

        with st.expander("ℹ️ Contact"):
            st.markdown(
                """

        **Need help?**  
        Please contact:

        - jerry_yang@ssg.gov.sg
        - lois@ssg.gov.sg
        - yee_sen@ssg.gov.sg
            """
            )
