import streamlit as st
import pandas as pd
from components.ui import *


def load_checkpoint_ui():
    create_header()
    st.header("🔄 Load Previous Checkpoint")
    st.markdown(
        """
        Resume your work by loading your previously saved checkpoint.
        Click the button below to retrieve and restore your last session.
        """
    )


# Enhanced function to show dataframe with better styling
def show_dataframe(
    df: pd.DataFrame, title: str, key: str, preview_rows: int | None = None
):
    """
    Display a dataframe (or just the first `preview_rows` rows) with a subheader
    and a CSV download button with enhanced styling.
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
    for i in range(3):
        dataframe_to_display = dfs[i][0]
        display_title = dfs[i][1]
        show_dataframe(
            dataframe_to_display, title=display_title, key=f"res{i+1}", preview_rows=4
        )
