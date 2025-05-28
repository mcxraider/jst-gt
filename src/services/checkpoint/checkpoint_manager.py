import streamlit as st
from datetime import datetime
from pathlib import Path

from config import CHECKPOINT_PATH
from services.storage import *


class CheckpointManager:
    """
    Manages saving and loading of pipeline state to a pickle file.
    Abstracts I/O for easy migration to S3 or local storage.
    
    This class handles the persistence of processing state between pipeline runs,
    allowing for resumption of interrupted processing. It manages both local and
    S3 storage of checkpoint data.
    
    Attributes:
        base_checkpoint_path (str): Base directory for checkpoint storage
        checkpoint_path (str): Full path to the current checkpoint file
        state (dict): Current state of the processing pipeline
        last_progress (float): Last recorded progress (0-1)
        current_round (str): Current processing round ('r1' or 'r2')
        sector (str): Current sector being processed
    """

    def __init__(self, alias: str, TIMESTAMP: str, checkpoint_dir=None):
        """
        Initialize the CheckpointManager with process alias and timestamp.
        
        Args:
            alias (str): Process alias used in checkpoint filename
            TIMESTAMP (str): Timestamp string for checkpoint versioning
            checkpoint_dir (str, optional): Custom checkpoint directory path
        """
        if checkpoint_dir is None:
            # Default to config path (can be S3 or local)
            checkpoint_dir = CHECKPOINT_PATH
        self.base_checkpoint_path = str(checkpoint_dir)
        filename = f"{alias}_checkpoint_{TIMESTAMP}.pkl"
        self.checkpoint_path = f"{self.base_checkpoint_path}/{filename}"
        self.state = {}
        self.last_progress = 0
        self.current_round = None
        self.sector = alias

    def load(self) -> bool:
        """
        Load the most recent checkpoint from storage.
        
        Searches for the most recent .pkl file in the checkpoint directory
        and loads its contents into the current state. Updates internal
        state variables with loaded data.
        
        Returns:
            bool: True if checkpoint was successfully loaded, False if no checkpoint exists
            
        Note:
            For local storage, uses file modification time to find latest checkpoint.
            For S3 storage, uses lexicographical ordering of filenames.
        """
        # List all .pkl files
        pkl_files = list_files(self.base_checkpoint_path, "*.pkl")
        if not pkl_files:
            return False

        # Find most recently modified (for S3, you might want to sort by filename or implement S3 last-modified)
        # Here, we assume lexicographical order if S3, timestamped filename
        if isinstance(pkl_files[0], Path):
            latest_file = max(pkl_files, key=lambda p: p.stat().st_mtime)
            latest_file = str(latest_file)
        else:
            # S3: use the latest by filename (relies on TIMESTAMP in name)
            latest_file = sorted(pkl_files)[-1]
        self.checkpoint_path = latest_file

        with st.spinner("Retrieving data from previously saved checkpoint"):
            self.state = load_pickle(latest_file)

        print(f"[Checkpoint] Loaded state from {latest_file}")
        self.last_progress = self.state.get("progress", self.last_progress)
        self.current_round = self.state.get("round", self.current_round)
        self.sector = self.state.get("sector", self.sector)
        st.session_state.selected_process_alias = self.sector
        return True

    def save(self):
        """
        Save current state to checkpoint file.
        
        Calculates progress based on pending and completed items,
        updates state with current sector and progress information,
        and saves the state to a pickle file. Also updates the
        Streamlit session state to indicate successful save.
        
        Note:
            Progress is calculated differently for round 1 and round 2
            based on the presence of respective pending and results lists.
        """
        # Calculate and store progress information
        if "r1_pending" in self.state and "r1_results" in self.state:
            total = len(self.state["r1_pending"]) + len(self.state["r1_results"])
            if total > 0:
                self.last_progress = len(self.state["r1_results"]) / total
                self.state["progress"] = self.last_progress
        elif "r2_pending" in self.state and "r2_results" in self.state:
            total = len(self.state["r2_pending"]) + len(self.state["r2_results"])
            if total > 0:
                self.last_progress = len(self.state["r2_results"]) / total
                self.state["progress"] = self.last_progress

        self.state["sector"] = st.session_state.selected_process_alias

        save_pickle(self.state, self.checkpoint_path)
        print(f"[Checkpoint] Saved state at {datetime.now()}")

        st.session_state.pkl_yes = True
