import streamlit as st
from pathlib import Path
from typing import Dict, Optional, Tuple
from datetime import datetime, timedelta, timezone
import hashlib
import sys

PASSWORD_KEY="ssgsail"
APP_NAME = "skill-proficiency-ai-tagger"

# Add src directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config import AUTH_DIR

from utils.session_cache import (
    load_session,
    update_session_activity,
    delete_session,
    cleanup_expired_sessions,
    find_active_session_by_email,
    list_active_sessions,
)

# Configuration - Store auth files locally, not in S3 output
PROJECT_ROOT = Path(__file__).parent.parent.parent  # Go up to project root
AUTH_DIR_PATH = PROJECT_ROOT / AUTH_DIR.lstrip("../")  # Remove ../ prefix and resolve


def get_next_hour_utc_timestamp_and_string():
    """Get the next hour UTC timestamp as string"""
    now_utc = datetime.now(timezone.utc)

    # Round up to the next full hour
    if now_utc.minute == 0 and now_utc.second == 0 and now_utc.microsecond == 0:
        next_hour_utc = now_utc
    else:
        next_hour_utc = now_utc.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

    # Get Unix timestamp
    unix_timestamp = int(next_hour_utc.timestamp())
    return str(unix_timestamp)


def get_current_hour_utc_timestamp_and_string():
    """Get the current hour UTC timestamp as string"""
    now_utc = datetime.now(timezone.utc)
    current_hour_utc = now_utc.replace(minute=0, second=0, microsecond=0)
    unix_timestamp = int(current_hour_utc.timestamp())
    return str(unix_timestamp)


def hash_password(text: str) -> str:
    """Return the SHA-256 hash of the input string."""
    sha_signature = hashlib.sha256(text.encode()).hexdigest()
    return sha_signature


def generate_valid_passwords():
    """Generate both current and next hour valid passwords"""
    current_time = get_current_hour_utc_timestamp_and_string()
    next_time = get_next_hour_utc_timestamp_and_string()

    # Generate current hour password
    current_combined = PASSWORD_KEY + current_time + APP_NAME
    current_password = hash_password(current_combined)

    # Generate next hour password
    next_combined = PASSWORD_KEY + next_time + APP_NAME
    next_password = hash_password(next_combined)

    return [current_password, next_password]


def authenticate_user(username: str, password: str) -> Tuple[bool, Optional[Dict]]:
    """Authenticate user using simple password or time-based password authentication"""

    # Generate valid time-based passwords
    valid_passwords = generate_valid_passwords()

    # Check if password matches any valid time-based password
    if password in valid_passwords:
        user_data = {
            "api_key": "ssg-time-auth",
            "role": "user",
            "email": username,
            "created_at": datetime.now().isoformat(),
        }
        return True, user_data

    # Fallback: Simple authentication for testing
    if password == "ssgiddapp":
        user_data = {
            "api_key": "ssg-simple-auth",
            "role": "user",
            "email": username,
            "created_at": datetime.now().isoformat(),
        }
        return True, user_data

    return False, None


def is_authenticated() -> bool:
    """Check if user is authenticated, including session cache validation"""
    # Clean up expired sessions first
    cleanup_expired_sessions()

    # Check current session state
    if st.session_state.get("authenticated", False):
        # Update activity for current session
        session_id = st.session_state.get("session_id")
        if session_id:
            update_session_activity(session_id)
        return True

    # If not authenticated in session state, check for cached sessions
    # First, try to restore from session_id if available
    session_id = st.session_state.get("session_id")
    if session_id:
        session_data = load_session(session_id)
        if session_data:
            # Restore session
            _restore_session_state(session_data, session_id)
            return True

    # Second, check if user has a stored email (for persistence across page refreshes)
    stored_email = st.session_state.get("stored_email")
    if stored_email:
        active_session_id = find_active_session_by_email(stored_email)
        if active_session_id:
            session_data = load_session(active_session_id)
            if session_data:
                # Restore session
                _restore_session_state(session_data, active_session_id)
                return True

    # Last resort: check for ANY active session (this handles complete session state loss)
    # This is important for handling page refreshes where session state is completely reset
    active_sessions = list_active_sessions()
    
    if active_sessions:
        # Get the most recent session
        most_recent_session = None
        most_recent_time = None
        
        for session_id, session_info in active_sessions.items():
            last_activity = datetime.fromisoformat(session_info["last_activity"])
            if most_recent_time is None or last_activity > most_recent_time:
                most_recent_time = last_activity
                most_recent_session = session_id
        
        if most_recent_session:
            session_data = load_session(most_recent_session)
            if session_data:
                # Restore the most recent session
                _restore_session_state(session_data, most_recent_session)
                return True

    return False


def _restore_session_state(session_data: Dict, session_id: str):
    """Helper function to restore complete session state including app state"""
    # Restore authentication state
    st.session_state.authenticated = True
    st.session_state.user_info = session_data["user_info"]
    st.session_state.username = session_data["email"].split("@")[0]
    st.session_state.session_id = session_id
    st.session_state.stored_email = session_data["email"]
    
    # Import here to avoid circular imports
    import sys
    import os
    from pathlib import Path
    
    # Temporarily change to the src directory to ensure proper path resolution
    original_cwd = os.getcwd()
    src_dir = Path(__file__).parent.parent
    os.chdir(src_dir)
    
    try:
        sys.path.append(str(src_dir))
        from services.db import check_pkl_existence, check_output_existence
        
        # Restore application state - check for output files
        st.session_state.csv_yes = check_output_existence()
        st.session_state.pkl_yes = check_pkl_existence()
    finally:
        # Always restore the original working directory
        os.chdir(original_cwd)


def get_current_user() -> Optional[Dict]:
    """Get current user info"""
    return st.session_state.get("user_info", None)


def logout():
    """Logout current user and clear session cache"""
    session_id = st.session_state.get("session_id")
    if session_id:
        delete_session(session_id)

    st.session_state.authenticated = False
    st.session_state.user_info = None
    st.session_state.username = None
    st.session_state.session_id = None
    st.session_state.stored_email = None  # Clear stored email
    
    # Trigger page rerun to redirect to login page
    st.rerun()
