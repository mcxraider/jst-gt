import streamlit as st
from utils.output_handler import *
from services.db.db import *
from components.page_header import *


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

            dfs = fetch_completed_output()
            view_download_csvs(dfs)

    else:
        st.error(
            """
   ⚠️ No results available to download.
        """
        )
