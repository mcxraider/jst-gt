"""Login form component"""

import streamlit as st
import time
from utils.db_handler import authenticate_user
from utils.session_cache import save_session


def login_form():
    """Render the login form with email and password inputs"""
    # email = st.text_input("E-mail", placeholder="Enter your SSG email address")
    email = "Test user"
    password = st.text_input(
        "Password", placeholder="Enter your copied password", type="password"
    )

    if st.button("Login", use_container_width=True):
        if not (password):
            st.error("You must have a password to Log in!")
        elif authenticate_user(email, password):
            # Create user info
            user_info = {"email": email, "role": "user", "login_time": time.time()}

            # Save session to cache
            session_id = save_session(email, user_info)

            # Set session state
            st.session_state["authenticated"] = True
            st.session_state["user_info"] = user_info
            st.session_state["username"] = email.split("@")[0]
            st.session_state["session_id"] = session_id
            st.session_state["stored_email"] = email  # Store email for persistence
            st.session_state["page"] = "app"
            st.rerun()
        else:
            st.error("Invalid login credentials")
