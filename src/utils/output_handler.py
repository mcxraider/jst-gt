import streamlit as st
import pandas as pd
import numpy as np
from typing import Optional, Tuple, List, Any
import time
import os
import datetime

def load_checkpoint_ui():
    st.header("🔄 Load Previous Checkpoint")
    st.markdown(
        """
        Resume your work by loading your previously saved checkpoint.
        Click the button below to retrieve and restore your last session.
        """
    )


# preview the dataframe and also make it available for download 
def show_dataframe(
    df: pd.DataFrame,
    title: str,
    key: str,
    preview_rows: int | None = None
):
    """
    Display a dataframe (or just the first `preview_rows` rows) with a subheader
    and a CSV download button.

    Args:
      df: full DataFrame
      title: title displayed above the table
      key: Streamlit key for the download button
      preview_rows: if set, only df.head(preview_rows) will be shown.
    """
    # choose what to show
    preview_df = df if preview_rows is None else df.head(preview_rows)

    col_display, col_download = st.columns([3, 1])
    with col_display:
        st.subheader(title)
        st.dataframe(preview_df)

    with col_download:
        st.download_button(
            label="Download CSV",
            data=df.to_csv(index=False).encode("utf-8"),  # downloads the full df
            file_name=f"{title}.csv",
            mime="text/csv",
            key=key,
        )


def view_download_csvs(dfs): # a tuple containing 3 tuples
    for i in range(3):
        dataframe_to_display = dfs[0][0]
        display_title = dfs[0][1]
        show_dataframe(dataframe_to_display, 
                       title=display_title, 
                       key=f"res{i+1}",
                       preview_rows = 4)


async def rename_output_file(file_name: str) -> str:
    """
    Asynchronously renames the file by appending a timestamp and 'output' before the file extension.
    """
    base, ext = os.path.splitext(file_name)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    new_name = f"{base}_{timestamp}_output{ext}"
    return new_name

