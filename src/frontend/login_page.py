import streamlit as st
from frontend.components.login import (
    login_header,
    login_form,
)
from utils.time_auth_utils import generate_valid_passwords
from utils.health_check import check_openai_api_health, check_s3_health


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

            # --- System Health Status ---
            st.write("---")
            st.write("**System Health**")

            openai_healthy = check_openai_api_health()
            s3_healthy = check_s3_health()

            if openai_healthy:
                st.write("OpenAI API: 🟢 Healthy")
            else:
                st.write("OpenAI API: 🔴 Unhealthy")

            if s3_healthy:
                st.write("S3 Bucket: 🟢 Healthy")
            else:
                st.write("S3 Bucket: 🔴 Unhealthy")
            # -----------------------------

            # Add bottom spacing
            st.write("")
            st.write("")
