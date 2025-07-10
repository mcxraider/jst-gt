import streamlit as st
from frontend.components.login import (
    login_header,
    login_form,
)
from utils.time_auth_utils import generate_valid_passwords
from frontend.components.login.system_health import (
    check_all_systems_health,
    display_system_health,
)


def simulate_password_provision():
    """Display the current valid time-based passwords for authentication"""
    valid_passwords = generate_valid_passwords()

    # Display current hour password (primary)
    st.write("**Simulated Password:**")
    st.code(valid_passwords[0], language="text")


def login_page():
    """Main login page with modular components"""

    with st.empty().container(border=False):
        _, col2, _ = st.columns([3, 4, 3])

        with col2:
            # Add top spacing
            st.write("")
            st.write("")

            # Header section
            login_header()

            # Perform health check in the background
            all_systems_healthy, openai_healthy, s3_healthy = check_all_systems_health()

            # Login form
            login_form(disabled=not all_systems_healthy)

            simulate_password_provision()

            # Display health status at the bottom
            display_system_health(openai_healthy, s3_healthy)

            # Add bottom spacing
            st.write("")
            st.write("")
