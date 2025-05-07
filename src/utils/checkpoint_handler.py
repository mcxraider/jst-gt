import streamlit as st
import pandas as pd
import numpy as np
from typing import Optional, Tuple, List, Any
import os
import time
from utils.processing import *
from utils.output_handler import *


def load_checkpoint_ui():
    st.header("🔄 Load Previous Checkpoint")
    st.markdown(
        """
        Resume your work by loading your previously saved checkpoint.
        Click the button below to retrieve and restore your last session.
        """
    )


def retrieve_checkpoint_metadata():
    time.sleep(5)


def resume_from_checkpoint():
    """Simulate loading data from a saved checkpoint (replace with real logic)."""
    time.sleep(5)
    df1 = pd.DataFrame(np.random.randn(5, 3), columns=["A", "B", "C"])
    df2 = pd.DataFrame(np.random.randint(0, 100, size=(5, 3)), columns=["D", "E", "F"])
    df3 = pd.DataFrame(
        np.random.choice(["X", "Y", "Z"], size=(5, 3)), columns=["G", "H", "I"]
    )
    return df1, df2, df3


def preview_results(df1, df2, df3):
    # Display loaded dataframes
    st.subheader("Result CSV 1")
    st.dataframe(df1)
    st.subheader("Result CSV 2")
    st.dataframe(df2)
    st.subheader("Result CSV 3")
    st.dataframe(df3)
    st.balloons()


def load_checkpoint_pipeline():
    """Simulates the 'Load Checkpoint' pipeline, assumes checkpoint yields Pandas DFs."""

    load_checkpoint_ui()

    # Load Checkpoint Button
    if st.button("Load Checkpoint", use_container_width=True):

        with st.spinner("Retrieving data from previously saved checkpoint"):
            retrieve_checkpoint_metadata()
        st.success("✅ Checkpoint data loaded successfully!")

        with st.spinner("Continuing job run from previous checkpoint..."):
            df1, df2, df3 = resume_from_checkpoint()
        st.success("✅ Tagging successful!")

        preview_results(df1, df2, df3)

        st.session_state.csv_yes = True
        st.subheader("Download Processed CSVs")
        col4, col5, col6 = st.columns(3)
        with col4:
            download_dataframe_as_csv(df1, "Result CSV 1", "res1")
        with col5:
            download_dataframe_as_csv(df2, "Result CSV 2", "res2")
        with col6:
            download_dataframe_as_csv(df3, "Result CSV 3", "res3")
