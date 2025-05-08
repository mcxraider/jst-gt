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
    Supports early exit if user toggles 'exit_halfway'.
    """
    ckpt_path = Path("../s3_bucket/s3_checkpoint/ckpt.pkl").resolve()

    st.write("Running core processing...")
    progress_bar = st.progress(0)
    num_rows = 6

    st.info("Checkpoints will be saved every 2 iterations.")
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
            }
            with open(ckpt_path, "wb") as f:
                pickle.dump(checkpoint_data, f)

        # early exit if toggle is on
        if st.session_state.get("exit_halfway", False) and i==3:
            st.warning("Processing stopped halfway by user toggle.")
            return []

    st.success("Core processing complete!")

    # simulate completion and return outputs
    df1, df2, df3 = fetch_completed_output()
    return [df1, df2, df3]

