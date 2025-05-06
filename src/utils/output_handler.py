import streamlit as st
import pandas as pd
import numpy as np
from typing import Optional, Tuple, List, Any
import os


def download_dataframe_as_csv(df: pd.DataFrame, label: str, key_suffix: str):
    """Provide a download button for a given Pandas DataFrame."""
    try:
        csv_string = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label=f"Download {label}",
            data=csv_string,
            file_name=f"{label.lower().replace(' ', '_')}.csv",
            mime="text/csv",
            key=f"download_{key_suffix}",
        )
    except Exception as e:
        st.error(f"Failed to prepare download for {label}: {e}")
