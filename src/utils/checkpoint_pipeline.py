import streamlit as st
import pandas as pd
import numpy as np
from typing import Optional, Tuple, List, Any
import os
import time
from utils.processing import *
from utils.output_handler import *
from utils.upload_pipeline import *


def retrieve_checkpoint_metadata():
    time.sleep(1)


def load_checkpoint_pipeline():
    """Simulates the 'Load Checkpoint' pipeline, assumes checkpoint yields Pandas DFs."""

    load_checkpoint_ui()

    # Load Checkpoint Button
    if st.button("Load Checkpoint", use_container_width=True):

        with st.spinner("Retrieving data from previously saved checkpoint"):
            retrieve_checkpoint_metadata()
        st.success("✅ Checkpoint metadata retrieved!")

        with st.spinner("Resuming from checkpoint…"):
            results = handle_checkpoint_processing()
        
        # handle early exit
        if not results:
            handle_exit("exit_from_failed_checkpoint")
            return
        
        st.success("✅ Tagging successful!")

        st.session_state.csv_yes = True

        st.subheader("View and Download Processed CSVs")

        view_download_csvs(results)
        st.balloons()
