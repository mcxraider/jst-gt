import streamlit as st
import pandas as pd
import numpy as np
from typing import Optional, Tuple, List, Any
import os
import time
from datetime import datetime


def random_df(n_rows: int, prefix: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": range(1, n_rows + 1),
            f"{prefix}_float": np.random.rand(n_rows),
            f"{prefix}_int": np.random.randint(1, 100, size=n_rows),
            f"{prefix}_category": np.random.choice(["X", "Y", "Z"], size=n_rows),
        }
    )


def handle_core_processing(
    *args: Any, **kwargs: Any
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Simulates the core data processing logic using Pandas.
    Generates and returns three pandas DataFrames.
    """

    st.write("Running core processing...")
    progress_bar = st.progress(0)
    num_rows = 20
    for i in range(num_rows):
        time.sleep(0.5)
        progress_bar.progress((i + 1) / num_rows)

    df1 = random_df(10, "res1")
    df2 = random_df(8, "res2")
    df3 = random_df(12, "res3")
    st.success("Core processing complete!")
    return df1, df2, df3


def handle_core_processing(
    *args: Any, **kwargs: Any
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Simulates the core data processing logic using Pandas.
    Generates and returns three pandas DataFrames.
    """

    st.write("Running core processing...")
    progress_bar = st.progress(0)
    num_rows = 20
    for i in range(num_rows):
        time.sleep(0.5)
        progress_bar.progress((i + 1) / num_rows)

    df1 = random_df(10, "res1")
    df2 = random_df(8, "res2")
    df3 = random_df(12, "res3")
    st.success("Core processing complete!")
    return df1, df2, df3
