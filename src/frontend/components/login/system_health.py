import streamlit as st
from utils.health_check import check_openai_api_health, check_s3_health
from config import USE_S3


def check_all_systems_health():
    """
    Performs health checks for all critical systems.
    Returns a tuple: (all_systems_healthy, openai_healthy, s3_healthy)
    """
    if USE_S3:
        openai_healthy = check_openai_api_health()
        s3_healthy = check_s3_health()
    else:
        openai_healthy = True
        s3_healthy = True

    all_healthy = openai_healthy and s3_healthy
    return all_healthy, openai_healthy, s3_healthy


def display_system_health(openai_healthy: bool, s3_healthy: bool):
    """Displays the system health status for OpenAI and S3 clients."""
    openai_status = "🟢 Healthy" if openai_healthy else "🔴 Unhealthy"
    s3_status = "🟢 Healthy" if s3_healthy else "🔴 Unhealthy"

    st.markdown(
        f"<p style='text-align: center;'>OpenAI Client: {openai_status}</p>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<p style='text-align: center;'>AWS S3 Client: {s3_status}</p>",
        unsafe_allow_html=True,
    )

    all_healthy = openai_healthy and s3_healthy
    if not all_healthy:
        st.markdown(
            "<p style='text-align: center; font-size: small;'>We're preventing you from logging in due to a failed system.</p>",
            unsafe_allow_html=True,
        )
