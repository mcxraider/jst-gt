import streamlit as st
from utils.session_handler import *


def back_homepage_button():
    if st.button("↩️ Home"):
        current_pkl_state = st.session_state.pkl_yes
        init_session_state()
        st.session_state.pkl_yes = current_pkl_state
        st.session_state.app_stage = "initial_choice"
        st.rerun()
