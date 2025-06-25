import streamlit as st
from frontend.components.login import (
    login_header,
    login_form,
    security_notice,
    guest_access,
)


def login_page(guest_mode=False):
    """Main login page with modular components"""

    with st.empty().container(border=True):
        col1, col2, col3 = st.columns([3, 4, 3])

        with col2:
            # Add top spacing
            st.write("")
            st.write("")

            # Header section
            login_header()

            # Login form
            login_form()

            # Security notice
            security_notice()

            # Guest access (if enabled)
            if guest_mode:
                guest_access()

            # Add bottom spacing
            st.write("")
            st.write("")
