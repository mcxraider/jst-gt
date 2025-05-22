import streamlit as st
from utils.output_handler import *
from utils.db import *
from components.buttons import *
from components.header import *


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


# Enhanced homepage with visual appeal
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


# Enhanced results page
def results_page():
    create_header()
    st.markdown(
        """
    <div class="css-card">
        <h2 style="margin-top: 0;">Results</h2>
    </div>
    """,
        unsafe_allow_html=True,
    )

    if st.session_state.csv_yes and st.session_state.results:
        st.info(
            """
            ✅ Preview and download your results
        """
        )
        with st.expander("📂 Preview Results"):

            dfs = st.session_state.results
            view_download_csvs(dfs)
            back_homepage_button()

    else:
        st.error(
            """
   ⚠️ No results available to download.
        """
        )
        back_homepage_button()