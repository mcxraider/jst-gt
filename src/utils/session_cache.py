"""Session cache utility for persistent authentication"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional
import hashlib
import sys

# Add src directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config import SESSION_TIMEOUT_HOURS, SESSIONS_DIR

# Configuration
PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / SESSIONS_DIR.lstrip("../")  # Remove ../ prefix and resolve


def ensure_cache_dir():
    """Ensure the cache directory exists"""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def generate_session_id(email: str) -> str:
    """Generate a unique session ID based on email and timestamp"""
    timestamp = datetime.now().isoformat()
    combined = f"{email}_{timestamp}"
    return hashlib.md5(combined.encode()).hexdigest()


def save_session(email: str, user_info: Dict) -> str:
    """Save user session to cache and return session ID"""
    ensure_cache_dir()

    # First, clean up any existing sessions for this email
    cleanup_user_sessions(email)

    session_id = generate_session_id(email)
    session_data = {
        "email": email,
        "user_info": user_info,
        "login_time": datetime.now().isoformat(),
        "last_activity": datetime.now().isoformat(),
    }

    session_file = CACHE_DIR / f"{session_id}.json"
    with open(session_file, "w") as f:
        json.dump(session_data, f, indent=2)

    # Also create a mapping file for email to session ID
    email_mapping_file = CACHE_DIR / "email_sessions.json"
    try:
        if email_mapping_file.exists():
            with open(email_mapping_file, "r") as f:
                email_sessions = json.load(f)
        else:
            email_sessions = {}
    except json.JSONDecodeError:
        email_sessions = {}

    email_sessions[email] = session_id

    with open(email_mapping_file, "w") as f:
        json.dump(email_sessions, f, indent=2)

    return session_id


def find_active_session_by_email(email: str) -> Optional[str]:
    """Find an active session ID for a given email"""
    ensure_cache_dir()

    email_mapping_file = CACHE_DIR / "email_sessions.json"
    if not email_mapping_file.exists():
        return None

    try:
        with open(email_mapping_file, "r") as f:
            email_sessions = json.load(f)

        session_id = email_sessions.get(email)
        if session_id and load_session(session_id):
            return session_id
    except json.JSONDecodeError:
        pass

    return None


def cleanup_user_sessions(email: str):
    """Clean up existing sessions for a user"""
    ensure_cache_dir()

    # Clean up from email mapping
    email_mapping_file = CACHE_DIR / "email_sessions.json"
    if email_mapping_file.exists():
        try:
            with open(email_mapping_file, "r") as f:
                email_sessions = json.load(f)

            old_session_id = email_sessions.get(email)
            if old_session_id:
                delete_session(old_session_id)
                del email_sessions[email]

                with open(email_mapping_file, "w") as f:
                    json.dump(email_sessions, f, indent=2)
        except json.JSONDecodeError:
            pass


def load_session(session_id: str) -> Optional[Dict]:
    """Load session data from cache if valid"""
    if not session_id:
        return None

    session_file = CACHE_DIR / f"{session_id}.json"
    if not session_file.exists():
        return None

    try:
        with open(session_file, "r") as f:
            session_data = json.load(f)

        # Check if session has expired
        last_activity = datetime.fromisoformat(session_data["last_activity"])
        if datetime.now() - last_activity > timedelta(hours=SESSION_TIMEOUT_HOURS):
            # Session expired, delete it
            session_file.unlink()
            return None

        # Update last activity time
        session_data["last_activity"] = datetime.now().isoformat()
        with open(session_file, "w") as f:
            json.dump(session_data, f, indent=2)

        return session_data

    except (json.JSONDecodeError, KeyError, ValueError):
        # Invalid session file, delete it
        if session_file.exists():
            session_file.unlink()
        return None


def update_session_activity(session_id: str):
    """Update the last activity time for a session"""
    if not session_id:
        return

    session_file = CACHE_DIR / f"{session_id}.json"
    if not session_file.exists():
        return

    try:
        with open(session_file, "r") as f:
            session_data = json.load(f)

        session_data["last_activity"] = datetime.now().isoformat()

        with open(session_file, "w") as f:
            json.dump(session_data, f, indent=2)

    except (json.JSONDecodeError, FileNotFoundError):
        pass


def delete_session(session_id: str):
    """Delete a session from cache and email mapping"""
    if not session_id:
        return

    # First get the email from the session before deleting
    session_file = CACHE_DIR / f"{session_id}.json"
    email = None
    if session_file.exists():
        try:
            with open(session_file, "r") as f:
                session_data = json.load(f)
            email = session_data.get("email")
        except json.JSONDecodeError:
            pass

        session_file.unlink()

    # Remove from email mapping
    if email:
        email_mapping_file = CACHE_DIR / "email_sessions.json"
        if email_mapping_file.exists():
            try:
                with open(email_mapping_file, "r") as f:
                    email_sessions = json.load(f)

                if email in email_sessions and email_sessions[email] == session_id:
                    del email_sessions[email]
                    with open(email_mapping_file, "w") as f:
                        json.dump(email_sessions, f, indent=2)
            except json.JSONDecodeError:
                pass


def cleanup_expired_sessions():
    """Clean up expired session files"""
    ensure_cache_dir()

    current_time = datetime.now()
    for session_file in CACHE_DIR.glob("*.json"):
        try:
            with open(session_file, "r") as f:
                session_data = json.load(f)

            last_activity = datetime.fromisoformat(session_data["last_activity"])
            if current_time - last_activity > timedelta(hours=SESSION_TIMEOUT_HOURS):
                session_file.unlink()

        except (json.JSONDecodeError, KeyError, ValueError, FileNotFoundError):
            # Invalid file, delete it
            if session_file.exists():
                session_file.unlink()


def get_session_info(session_id: str) -> Optional[Dict]:
    """Get session information including time remaining"""
    session_data = load_session(session_id)
    if not session_data:
        return None

    last_activity = datetime.fromisoformat(session_data["last_activity"])
    time_remaining = timedelta(hours=SESSION_TIMEOUT_HOURS) - (
        datetime.now() - last_activity
    )

    return {
        "email": session_data["email"],
        "login_time": session_data["login_time"],
        "last_activity": session_data["last_activity"],
        "time_remaining_minutes": int(time_remaining.total_seconds() / 60),
        "expires_at": (
            last_activity + timedelta(hours=SESSION_TIMEOUT_HOURS)
        ).isoformat(),
    }


def list_active_sessions() -> Dict:
    """Debug function to list all active sessions"""
    ensure_cache_dir()
    sessions = {}

    for session_file in CACHE_DIR.glob("*.json"):
        if session_file.name == "email_sessions.json":
            continue
        try:
            session_data = load_session(session_file.stem)
            if session_data:
                sessions[session_file.stem] = {
                    "email": session_data["email"],
                    "login_time": session_data["login_time"],
                    "last_activity": session_data["last_activity"],
                }
        except:
            continue

    return sessions
