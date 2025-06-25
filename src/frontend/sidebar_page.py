import streamlit as st
from frontend.components.sidebar.sidebar_nav import sidebar_nav
from frontend.components.sidebar.sidebar_help import sidebar_help
from frontend.components.sidebar.sidebar_contact import sidebar_contact
from frontend.components.sidebar.sidebar_user import sidebar_user
from utils.time_auth_utils import is_authenticated


def sidebar():
    with st.sidebar:
        if is_authenticated():
            sidebar_nav()
            st.markdown("---")
            sidebar_help()
            sidebar_contact()
            sidebar_user()  # Add user info at the bottom
        else:
            st.markdown("### 🔐 Please Login")
            st.info("Access the application by logging in first.")
