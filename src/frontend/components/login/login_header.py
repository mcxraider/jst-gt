"""Login page header component"""
import streamlit as st

def login_header():
    """Render the login page header with title and subtitle"""
    st.markdown("""
    <div style="text-align: center; margin-bottom: 2rem;">
        <h1 style="color: #2c3e50; margin-bottom: 1rem;">Welcome to the Skills Proficiency Tagging App</h1>
        <p style="color: #7f8c8d; font-size: 1.1rem;">Enter your email and password to access the app</p>
    </div>
    """, unsafe_allow_html=True)
