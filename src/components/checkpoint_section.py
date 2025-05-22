import streamlit as st
from components.page_header import *


def load_checkpoint_ui():
    create_header()
    st.header("🔄 Load Previous Checkpoint")
    st.markdown(
        """
        Resume your work by loading your previously saved checkpoint.
        Click the button below to retrieve and restore your last session.
        """
    )
