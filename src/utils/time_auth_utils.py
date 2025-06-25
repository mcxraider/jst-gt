import streamlit as st
import hashlib
import json
from pathlib import Path
from typing import Dict, Optional, Tuple
from datetime import datetime, timedelta
import sys

# Add src directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config import AUTH_DIR, APP_NAME, VALIDITY_MINUTES

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
AUTH_FILE = AUTH_DIR_PATH / "api_keys.json"


def load_api_keys() -> Dict[str, Dict]:
    """Load API keys from JSON file"""
    if not AUTH_FILE.exists():
        # Ensure auth directory exists
        AUTH_DIR_PATH.mkdir(exist_ok=True)

        # Create default API key if none exist
        default_keys = {
            "user1": {
                "api_key": "sk-demo-abc123xyz789",
                "role": "admin",
                "email": "admin@example.com",
                "created_at": datetime.now().isoformat(),
            }
        }
        save_api_keys(default_keys)
        return default_keys

    try:
        with open(AUTH_FILE, "r") as f:
            return json.load(f)
    except:
        return {}


def save_api_keys(keys: Dict[str, Dict]) -> None:
    """Save API keys to JSON file"""
    AUTH_DIR_PATH.mkdir(exist_ok=True)
    with open(AUTH_FILE, "w") as f:
        json.dump(keys, f, indent=2)


def get_current_time_block() -> str:
    """Get current 5-minute time block as string"""
    now = datetime.now()
    # Round down to nearest 5-minute block
    minutes = (now.minute // VALIDITY_MINUTES) * VALIDITY_MINUTES
    time_block = now.replace(minute=minutes, second=0, microsecond=0)
    return time_block.strftime("%Y%m%d%H%M")


def generate_dynamic_password(api_key: str, time_block: Optional[str] = None) -> str:
    """Generate dynamic password using API key + app name + time block"""
    if time_block is None:
        time_block = get_current_time_block()

    # Combine API key + app name + time block
    combined_string = f"{api_key}{APP_NAME}{time_block}"

    # Generate hash
    password_hash = hashlib.sha256(combined_string.encode()).hexdigest()

    # Return first 12 characters for easier typing
    return password_hash[:12]


def get_valid_passwords_for_api_key(api_key: str) -> list:
    """Get all currently valid passwords for an API key (current and previous 5-min block)"""
    current_time_block = get_current_time_block()
    current_password = generate_dynamic_password(api_key, current_time_block)

    # Also check previous 5-minute block to handle edge cases
    prev_time = datetime.now() - timedelta(minutes=VALIDITY_MINUTES)
    prev_minutes = (prev_time.minute // VALIDITY_MINUTES) * VALIDITY_MINUTES
    prev_time_block = prev_time.replace(minute=prev_minutes, second=0, microsecond=0)
    prev_time_block_str = prev_time_block.strftime("%Y%m%d%H%M")
    prev_password = generate_dynamic_password(api_key, prev_time_block_str)

    return [current_password, prev_password]


def authenticate_user(username: str, password: str) -> Tuple[bool, Optional[Dict]]:
    """Authenticate user using simple password or time-based password (backward compatibility)"""

    # New simple authentication for @ssg.gov.sg emails
    if "@ssg.gov.sg" in username and password == "ssgiddapp":
        user_data = {
            "api_key": "ssg-simple-auth",
            "role": "user",
            "email": username,
            "created_at": datetime.now().isoformat(),
        }
        return True, user_data

    # Existing time-based authentication for backward compatibility
    api_keys = load_api_keys()

    if username not in api_keys:
        return False, None

    user_data = api_keys[username]
    api_key = user_data["api_key"]

    # Get valid passwords for this API key
    valid_passwords = get_valid_passwords_for_api_key(api_key)

    # Check if provided password matches any valid password
    if password in valid_passwords:
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


def get_next_password_time() -> str:
    """Get the time when the next password will be generated"""
    now = datetime.now()
    current_minutes = (now.minute // VALIDITY_MINUTES) * VALIDITY_MINUTES
    next_block = now.replace(
        minute=current_minutes, second=0, microsecond=0
    ) + timedelta(minutes=VALIDITY_MINUTES)
    return next_block.strftime("%H:%M")
