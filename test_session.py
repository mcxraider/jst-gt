"""Test session cache functionality"""
from utils.session_cache import save_session, load_session, list_active_sessions, find_active_session_by_email
from datetime import datetime

def test_session_cache():
    """Test the session cache functionality"""
    print("Testing session cache...")

    # Test saving a session
    user_info = {
        "email": "test@ssg.gov.sg",
        "role": "user",
        "login_time": datetime.now().isoformat()
    }

    session_id = save_session("test@ssg.gov.sg", user_info)
    print(f"Created session: {session_id}")

    # Test loading the session
    loaded_session = load_session(session_id)
    if loaded_session:
        print(f"Successfully loaded session for: {loaded_session['email']}")
    else:
        print("Failed to load session")

    # Test finding session by email
    found_session_id = find_active_session_by_email("test@ssg.gov.sg")
    if found_session_id:
        print(f"Found session by email: {found_session_id}")
    else:
        print("Could not find session by email")

    # List all active sessions
    active_sessions = list_active_sessions()
    print(f"Active sessions: {active_sessions}")

if __name__ == "__main__":
    test_session_cache()
