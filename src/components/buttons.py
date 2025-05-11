import streamlit as st
from utils.session_handler import *


def back_homepage_button():
    if st.button("↩️ Start Over"):
        current_pkl_state = st.session_state.pkl_yes
        init_session_state()
        st.session_state.pkl_yes = current_pkl_state
        st.session_state.app_stage = "initial_choice"
        st.rerun()


def back_homepage_from_failed_run_button(button_title: str):
    if st.button(f"↩️ Start Over", key=button_title):
        init_session_state()
        st.session_state.pkl_yes = True
        st.session_state.app_stage = "initial_choice"
        st.rerun()
