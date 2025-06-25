import streamlit as st
from pathlib import Path
from typing import Dict, Optional, Tuple
from datetime import datetime
import sys

# Add src directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config import AUTH_DIR

from utils.session_cache import (
    save_session,
    load_session,
    update_session_activity,
    delete_session,
    cleanup_expired_sessions,
    find_active_session_by_email,
)

# Configuration - Store auth files locally, not in S3 output
PROJECT_ROOT = Path(__file__).parent.parent.parent  # Go up to project root
AUTH_DIR_PATH = PROJECT_ROOT / AUTH_DIR.lstrip("../")  # Remove ../ prefix and resolve


def authenticate_user(username: str, password: str) -> Tuple[bool, Optional[Dict]]:
    """Authenticate user using simple password authentication"""

    # Simple authentication for @ssg.gov.sg emails
    if "@ssg.gov.sg" in username and password == "ssgiddapp":
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
    # Clean up expired sessions
    cleanup_expired_sessions()

    # Check current session state
    if st.session_state.get("authenticated", False):
        # Update activity for current session
        session_id = st.session_state.get("session_id")
        if session_id:
            update_session_activity(session_id)
        return True

    # Check if there's a cached session by session_id
    session_id = st.session_state.get("session_id")
    if session_id:
        session_data = load_session(session_id)
        if session_data:
            # Restore session
            st.session_state.authenticated = True
            st.session_state.user_info = session_data["user_info"]
            st.session_state.username = session_data["email"].split("@")[0]
            return True

    # Check if user has a stored email preference (for persistence across page refreshes)
    stored_email = st.session_state.get("stored_email")
    if stored_email:
        active_session_id = find_active_session_by_email(stored_email)
        if active_session_id:
            session_data = load_session(active_session_id)
            if session_data:
                # Restore session
                st.session_state.authenticated = True
                st.session_state.user_info = session_data["user_info"]
                st.session_state.username = session_data["email"].split("@")[0]
                st.session_state.session_id = active_session_id
                return True

    return False


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
