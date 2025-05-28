import streamlit as st

from frontend.components.page_header.page_header import create_header
from frontend.components.homepage.home_action_card import home_action_card
from frontend.components.homepage.homepage_status_messages import home_status_messages


def homepage():
    create_header()
    pkl_available = st.session_state.pkl_yes
    load_checkpoint_enabled = pkl_available and not st.session_state.csv_yes

    home_action_card(pkl_available, load_checkpoint_enabled)
    home_status_messages(pkl_available, load_checkpoint_enabled)
