import streamlit as st
# import utils app components
from utils.input_handler import *
from utils.session_handler import *
from utils.output_handler import *
from utils.checkpoint_pipeline import *
from utils.db import *
from utils.upload_pipeline import *
from components.buttons import *
from components.pages import *


def main():
    configure_page()
    init_session_state()
    
    demo_sidebar() # attach only when doing the demo

    # --- Initial Choice Stage ---
    if st.session_state.app_stage == "initial_choice":
        homepage()

    # --- Stage for Uploading/Configuring New Process ---
    elif st.session_state.app_stage == "uploading_new":
        upload_new_pipeline()

    elif st.session_state.app_stage == "load_checkpoint":
        load_checkpoint_pipeline()
        back_homepage_button()

    # --- Results Ready Stage ---
    elif st.session_state.app_stage == "results_ready":
        results_page()
        back_homepage_button()


if __name__ == "__main__":
    main()
