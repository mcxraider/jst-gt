import streamlit as st
from frontend.components.sidebar.sidebar_nav import sidebar_nav
from frontend.components.sidebar.sidebar_help import sidebar_help
from frontend.components.sidebar.sidebar_contact import sidebar_contact


def demo_sidebar():
    with st.sidebar:
        sidebar_nav()
        st.markdown("---")
        sidebar_help()
        sidebar_contact()
