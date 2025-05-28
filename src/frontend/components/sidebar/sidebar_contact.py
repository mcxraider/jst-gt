# frontend/components/sidebar_contact.py
import streamlit as st


def sidebar_contact():
    with st.expander("ℹ️ Contact"):
        st.markdown(
            """
            **Need help?**  
            Please contact:
            - jerry_yang_from.tp@tech.gov.sg
            """
        )
