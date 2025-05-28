# frontend/components/sector_selector.py
import streamlit as st
from config import PROCESS_CHOICES
from utils.upload_utils import get_process_alias, get_process


def sector_selector():
    st.markdown("<h3>Select a Sector:</h3>", unsafe_allow_html=True)
    selected_process = st.selectbox(
        "Select a process:",
        PROCESS_CHOICES,
        key="process_choice",
        help="Pick which pipeline you want to run.",
    )
    # Parse out the alias and display name
    alias = get_process_alias(selected_process)
    display = get_process(selected_process)
    st.session_state.selected_process_alias = alias
    st.session_state.selected_process = display
