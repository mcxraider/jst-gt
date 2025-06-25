"""Security notice component for login page"""

import streamlit as st


def security_notice():
    """Render the security notice below the login form"""
    st.markdown(
        """
    <div style="text-align: center; margin-top: 2rem; padding: 1rem; background-color: #f8f9fa; border-radius: 8px; border-left: 4px solid #28a745;">
        <div style="color: #495057; font-size: 0.9rem; margin-bottom: 0.5rem;">
            🔐 Secure access for SSG personnel only
        </div>
        <div style="color: #6c757d; font-size: 0.85rem;">
            Use your @ssg.gov.sg email address
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )
