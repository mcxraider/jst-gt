import streamlit as st
import pandas as pd
from typing import Any
import time
import pickle
from utils.db import *


def handle_core_processing(*args: Any, **kwargs: Any):
    """
    Simulates the core data processing logic using Pandas.
    Generates and returns three pandas DataFrames, and checkpoints progress.
    """
    ckpt_path = "../s3_bucket/s3_checkpoint/ckpt.pkl"

    st.write("Running core processing...")
    progress_bar = st.progress(0)
    num_rows = 6

    st.info(f"Checkpoint saved at every 3 min mark")
    for i in range(num_rows):
        # simulate work
        time.sleep(0.5)
        progress = (i + 1) / num_rows
        progress_bar.progress(progress)
        st.session_state.pkl_yes = True
        # checkpoint every 2 iterations
        if (i + 1) % 2 == 0:
            checkpoint_data = {
                "iteration": i + 1,
                "progress": progress,
                # you can add more state here, e.g. partial results
            }
            with open(ckpt_path, "wb") as f:
                pickle.dump(checkpoint_data, f)

    st.success("Core processing complete!")

    df1, df2, df3 = fetch_completed_output()

    return [df1, df2, df3]
