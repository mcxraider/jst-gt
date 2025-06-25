# frontend/components/sidebar_nav.py
import streamlit as st
from utils.session_handler import init_session_state
from utils.time_auth_utils import get_current_user


def sidebar_nav():
    st.header("Navigation")
    if st.button("🏠 Home", use_container_width=True):
        st.session_state.app_stage = "initial_choice"
        current_pkl_state = st.session_state.pkl_yes
        init_session_state()
        st.session_state.pkl_yes = current_pkl_state
        st.session_state.processing = False
        st.rerun()
    if st.session_state.csv_yes and st.session_state.results:
        if st.button("📊 Results", use_container_width=True):
            st.session_state.app_stage = "results_ready"
            st.rerun()

    # Admin section
    current_user = get_current_user()
    if current_user and current_user.get("role") == "admin":
        st.markdown("---")
        st.markdown("**⚙️ Admin**")
        if st.button("🔑 Manage API Keys", use_container_width=True):
            st.session_state.app_stage = "api_key_management"
            st.rerun()
