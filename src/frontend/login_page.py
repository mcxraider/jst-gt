import streamlit as st
from frontend.components.auth.login_form import login_form
from datetime import datetime

def login_page():
    """Main login page with time-based authentication"""
    # Page header
    st.markdown("""
    <div style="text-align: center; margin-bottom: 2rem;">
        <h1>🏷️ Skill Proficiency AI Tagger</h1>
        <p style="color: #666; font-size: 1.1rem;">Secure Time-Based Access Portal</p>
    </div>
    """, unsafe_allow_html=True)

    # Login form
    login_form()

    # Footer with time info
    current_time = datetime.now().strftime("%b %d, %Y • %I:%M:%S %p")

