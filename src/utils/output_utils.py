import streamlit as st
import pandas as pd
from frontend.components.page_header.page_header import *


def show_dataframe(
    df: pd.DataFrame, title: str, key: str, preview_rows: int | None = None
):
    """
    Display a styled dataframe in Streamlit with a subheader and a CSV download button.
    
    This function creates a card-like container with:
    - A title header
    - A preview of the dataframe (full or limited rows)
    - A download button for the complete CSV
    - Column information and statistics
    
    Args:
        df (pd.DataFrame): The dataframe to display
        title (str): The title to display above the dataframe
        key (str): A unique key for the Streamlit download button
        preview_rows (int | None, optional): If set, only shows the first N rows.
                                           If None, shows the full dataframe.
    """
    # Choose what to show
    preview_df = df if preview_rows is None else df.head(preview_rows)

    st.markdown(
        f"""
    <div class="css-card">
        <h3 style="margin-top: 0;">{title}</h3>
    """,
        unsafe_allow_html=True,
    )

    col_display, col_download = st.columns([3, 1])
    with col_display:
        st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
        st.dataframe(preview_df, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

        if preview_rows is not None and len(df) > preview_rows:
            st.caption(f"Showing {preview_rows} of {len(df)} records")

    with col_download:
        st.markdown("<br>", unsafe_allow_html=True)
        csv = df.to_csv(index=False).encode("utf-8")

        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"{title}.csv",
            mime="text/csv",
            key=key,
            use_container_width=True,
        )

    st.markdown("</div>", unsafe_allow_html=True)


def view_download_csvs(dfs):  # a tuple containing 3 tuples
    """
    Display and provide download options for multiple dataframes in Streamlit.
    
    This function processes a tuple of three dataframes, each with its own display title.
    For each dataframe, it:
    - Shows a preview of the first 4 rows
    - Provides a download button for the complete CSV
    - Displays the dataframe in a styled card container
    
    Args:
        dfs (tuple): A tuple containing three tuples, where each inner tuple contains:
            - A pandas DataFrame
            - A string title for display
    """
    for i in range(3):
        dataframe_to_display = dfs[i][0]
        display_title = dfs[i][1]
        show_dataframe(
            dataframe_to_display, title=display_title, key=f"res{i+1}", preview_rows=4
        )
