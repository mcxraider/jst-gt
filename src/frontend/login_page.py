import streamlit as st
from frontend.components.login import (
    login_header,
    login_form,
    security_notice,
)
from utils.time_auth_utils import generate_valid_passwords


def simulate_password_provision():
    """Display the current valid time-based passwords for authentication"""
    valid_passwords = generate_valid_passwords()

    # Display current hour password (primary)
    st.write("**Simulated Password:**")
    st.code(valid_passwords[0], language="text")


def login_page():
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

            simulate_password_provision()

            # Security notice
            # security_notice()

            # Add bottom spacing
            st.write("")
            st.write("")
