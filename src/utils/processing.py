import streamlit as st
import pandas as pd
from typing import Any
import time
import pickle
from utils.db import *
import random

num_rows = 10

def handle_core_processing(*args: Any, **kwargs: Any):
    """
    Simulates the core data processing logic using Pandas.
    Generates and returns three pandas DataFrames, and checkpoints progress.
    Supports early exit if user toggles 'exit_halfway'.
    """
    ckpt_path = Path("../s3_bucket/s3_checkpoint/ckpt.pkl").resolve()

    st.info("Running core processing, checkpoints will be saved every 2 minutes.")
    progress_bar = st.progress(0)

    for i in range(num_rows):
        # simulate work
        time.sleep(0.5)
        progress = (i + 1) / num_rows
        progress_bar.progress(progress)

        # checkpoint every 2 iterations
        if (i + 1) % 2 == 0:
            checkpoint_data = {
                "iteration": i + 1,
                "progress": progress,
            }
            with open(ckpt_path, "wb") as f:
                pickle.dump(checkpoint_data, f)
            st.session_state.pkl_yes = True


        # early exit if toggle is on
        stop_number = random.randint(1, num_rows)
        if st.session_state.get("exit_halfway", False) and i == stop_number:
            return []

    st.success("Core processing complete!")

    # simulate completion and return outputs
    df1, df2, df3 = fetch_completed_output()
    return [df1, df2, df3]


def handle_checkpoint_processing(*args: Any, **kwargs: Any):
    """
    Resumes processing from the last saved checkpoint in ckpt.pkl.
    Reads iteration and progress, then continues core processing from that point.
    Supports early exit via `exit_halfway` toggle.
    """
    ckpt_path = Path("../s3_bucket/s3_checkpoint/ckpt.pkl").resolve()

    # Load existing checkpoint or start fresh
    if ckpt_path.exists():
        try:
            with open(ckpt_path, "rb") as f:
                checkpoint_data = pickle.load(f)
            start_iter = checkpoint_data.get("iteration", 0)
            last_progress = checkpoint_data.get("progress", 0)
            st.info(f"Resuming at iteration {start_iter}, progress={last_progress*100:.1f}%.")
        except Exception as e:
            st.error(f"Failed to load checkpoint: {e}. Restarting from beginning.")
            start_iter = 0
            last_progress = 0
    else:
        st.warning("No checkpoint found. Starting from the beginning.")
        start_iter = 0
        last_progress = 0

    # Mark checkpoint available
    st.session_state.pkl_yes = True
    progress_bar = st.progress(last_progress)

    # Continue processing from last checkpoint
    for i in range(start_iter, num_rows):
        time.sleep(0.5)
        progress = (i + 1) / num_rows
        progress_bar.progress(progress)

        # update checkpoint every 2 iterations
        if (i + 1) % 2 == 0:
            checkpoint_data = {"iteration": i + 1, "progress": progress}
            with open(ckpt_path, "wb") as f:
                pickle.dump(checkpoint_data, f)

        # early exit if toggle is on
        stop_number = random.randint(1, num_rows)
        if st.session_state.get("exit_halfway", False) and i == stop_number:
            return []

    st.success("Core processing complete!")

    # simulate completion and return outputs
    df1, df2, df3 = fetch_completed_output()
    return [df1, df2, df3]    