import streamlit as st
from utils.time_auth_utils import get_current_user, logout


def sidebar_user():
    """Display user info and logout button in sidebar"""
    user_info = get_current_user()
    if user_info:
        st.markdown("---")
        st.markdown("**👤 User Info**")
        st.write(f"**Username:** {st.session_state.username}")
        st.write(f"**Role:** {user_info.get('role', 'User').title()}")

        if st.button("🚪 Logout", use_container_width=True):
            logout()
            st.rerun()
