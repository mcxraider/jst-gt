import streamlit as st

# Removed: import polars as pl
import pandas as pd  # Ensure pandas is imported
import numpy as np
from typing import Optional, Tuple, List, Any
import os

# Placeholder for actual API key handling if needed later
# OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY", "YOUR_DEFAULT_KEY")


def configure_page():
    """Set Streamlit page configuration and title."""
    page_title = "Proficiency Skills Tagging Processor"
    st.set_page_config(page_title=page_title, layout="wide")
    st.title(page_title)


def init_session_state():
    """Initialize session state variables."""
    for key in ("results", "error_msg"):
        if key not in st.session_state:
            st.session_state[key] = None

    if "app_stage" not in st.session_state:
        st.session_state["app_stage"] = "initial_choice"
    if "csv_yes" not in st.session_state:
        st.session_state["csv_yes"] = False
    if "pkl_yes" not in st.session_state:
        # Example: Check for a default checkpoint file
        st.session_state["pkl_yes"] = os.path.exists("my_checkpoint.pkl")


def upload_csv() -> Optional[pd.DataFrame]:
    """Display file uploader, read CSV using Pandas, with error handling."""
    uploaded = st.file_uploader("Upload a CSV file to process", type=["csv"])
    if not uploaded:
        return None
    try:
        df = pd.read_csv(uploaded)
        st.write("**Preview of uploaded data:**")
        st.dataframe(df.head())
        return df
    except Exception as e:
        st.error(f"Error loading CSV: {e}")
        return None


def get_user_options(df: pd.DataFrame) -> Optional[Tuple[List[str], str, str]]:
    """Collect processing options: columns, prompt template, and model."""
    st.subheader("Processing Options")
    cols = list(df.columns)
    selected = st.multiselect("Select column(s) to process", cols)
    prompt_template = st.text_area(
        "Prompt template (use {text} to insert values if applicable)",
        "Process the following data: {text}",
    )
    model_choice = st.selectbox(
        "Choose processing model/method", ["Method A", "Method B", "Method C"]
    )
    if not selected:
        st.warning("Please select at least one column.")
        return None
    return selected, prompt_template, model_choice


def handle_core_processing(
    *args: Any, **kwargs: Any
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Simulates the core data processing logic using Pandas.
    Generates and returns three pandas DataFrames.
    """
    st.write("Running core processing...")
    import time

    # prev_state = load_prevstate()
    progress_bar = st.progress(0)
    for i in range(4):
        time.sleep(0.5)
        progress_bar.progress((i + 1) / 4)

    def random_df(n_rows: int, prefix: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "id": range(1, n_rows + 1),
                f"{prefix}_float": np.random.rand(n_rows),
                f"{prefix}_int": np.random.randint(1, 100, size=n_rows),
                f"{prefix}_category": np.random.choice(["X", "Y", "Z"], size=n_rows),
            }
        )

    df1 = random_df(10, "res1")
    df2 = random_df(8, "res2")
    df3 = random_df(12, "res3")
    st.success("Core processing complete!")
    return df1, df2, df3


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


def upload_new_pipeline():
    """Simulates the 'Upload New File' pipeline using Pandas."""
    st.subheader("1. Upload Data")
    uploaded_df = upload_csv()

    if uploaded_df is not None:
        st.subheader("2. Configure Processing")
        options = get_user_options(uploaded_df)
        if options:
            selected_cols, prompt, model = options
            st.subheader("3. Start Processing")
            if st.button("Process Uploaded Data"):
                with st.spinner("Processing..."):
                    df1, df2, df3 = handle_core_processing()
                    st.session_state.results = (df1, df2, df3)
                    st.session_state.csv_yes = True
                    st.session_state.app_stage = "results_ready"
                    st.rerun()
            else:
                st.session_state.app_stage = "uploading_new"
                st.session_state.csv_yes = False
                st.session_state.results = None
        else:
            st.session_state.app_stage = "uploading_new"
            st.session_state.csv_yes = False
            st.session_state.results = None
    else:
        st.session_state.app_stage = "uploading_new"
        st.session_state.csv_yes = False
        st.session_state.results = None

    if st.button("Back to Choices"):
        st.session_state.app_stage = "initial_choice"
        st.session_state.csv_yes = False
        st.session_state.results = None
        st.rerun()


def load_checkpoint_pipeline():
    """Simulates the 'Load Checkpoint' pipeline, assumes checkpoint yields Pandas DFs."""
    st.write("Simulating loading from checkpoint...")
    with st.spinner("Loading checkpoint data..."):
        import time

        time.sleep(1.5)
        df1, df2, df3 = handle_core_processing()
        st.session_state.results = (df1, df2, df3)
        st.session_state.csv_yes = True
        st.session_state.app_stage = "results_ready"
        st.success("Checkpoint loaded successfully!")
    st.rerun()


def main():
    configure_page()
    init_session_state()

    # --- Simulation Control (Optional - For Demo) ---
    st.sidebar.header("Demo Controls")
    default_pkl_exists = os.path.exists("my_checkpoint.pkl")
    simulate_pkl = st.sidebar.checkbox(
        "Simulate Checkpoint Available (pkl_yes)", value=st.session_state.pkl_yes
    )
    st.session_state.pkl_yes = simulate_pkl
    simulate_csv = st.sidebar.checkbox(
        "Simulate CSV Processed (csv_yes)", value=st.session_state.csv_yes
    )
    st.session_state.csv_yes = simulate_csv
    # if user toggles csv_yes but no results, create placeholder DataFrames
    if st.session_state.csv_yes and st.session_state.results is None:
        st.session_state.results = (
            pd.DataFrame({"note": ["Demo CSV1"]}),
            pd.DataFrame({"note": ["Demo CSV2"]}),
            pd.DataFrame({"note": ["Demo CSV3"]}),
        )
    if st.sidebar.button("Reset App State"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()
    # --- End Simulation Control ---

    # --- Initial Choice Stage ---
    if st.session_state.app_stage == "initial_choice":
        st.header("Choose an action:")

        pkl_available = st.session_state.pkl_yes
        load_checkpoint_enabled = pkl_available and not st.session_state.csv_yes

        col1, col2 = st.columns(2)

        with col1:
            if st.button(
                "⬆️ Upload New File & Run process",
                key="upload_new",
                use_container_width=True,
            ):
                st.session_state.app_stage = "uploading_new"
                st.rerun()

        with col2:
            if st.button(
                "🔄 Load from Previous Checkpoint",
                key="load_checkpoint",
                disabled=not load_checkpoint_enabled,
                use_container_width=True,
            ):
                load_checkpoint_pipeline()

        if not load_checkpoint_enabled and pkl_available:
            already_done_processing_msg = "Your Previous Run Job has already been processed. Download them below or start a new session!"
            st.info(
                already_done_processing_msg
                )
        elif not pkl_available:
            st.info("Please upload the files for processing!")

        # Immediate download buttons if CSV processed
        if st.session_state.csv_yes:
            df1, df2, df3 = st.session_state.results
            st.subheader("Download Previously Processed CSVs")
            col4, col5, col6 = st.columns(3)
            with col4:
                download_dataframe_as_csv(df1, "Result CSV 1", "res1")
            with col5:
                download_dataframe_as_csv(df2, "Result CSV 2", "res2")
            with col6:
                download_dataframe_as_csv(df3, "Result CSV 3", "res3")

    # --- Stage for Uploading/Configuring New Process ---
    elif st.session_state.app_stage == "uploading_new":
        upload_new_pipeline()

    # --- Results Ready Stage ---
    elif st.session_state.app_stage == "results_ready":
        st.header("Results")
        if st.session_state.csv_yes and st.session_state.results:
            st.success("Processing complete. You can now download the results as CSV.")
            df1, df2, df3 = st.session_state.results

            st.subheader("Download Processed CSVs")
            col1, col2, col3 = st.columns(3)
            with col1:
                download_dataframe_as_csv(df1, "Result CSV 1", "res1")
            with col2:
                download_dataframe_as_csv(df2, "Result CSV 2", "res2")
            with col3:
                download_dataframe_as_csv(df3, "Result CSV 3", "res3")
        else:
            st.warning("No results available to download.")

        if st.button("↩️ Start Over"):
            current_pkl_state = st.session_state.pkl_yes
            for key in list(st.session_state.keys()):
                if key != "pkl_yes":
                    del st.session_state[key]
            init_session_state()
            st.session_state.pkl_yes = current_pkl_state
            st.session_state.app_stage = "initial_choice"
            st.rerun()


if __name__ == "__main__":
    main()
