# frontend/components/sidebar_help.py
import streamlit as st
from config import PDF_URL


def sidebar_help():
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
