import streamlit as st
from utils.time_auth_utils import get_current_user, logout


def sidebar_user():
    """Display user info and logout button in sidebar"""
    user_info = get_current_user()

    st.markdown("---")
    st.write("")

    if st.button("🚪 Logout", use_container_width=True):
        logout()
        st.rerun()
