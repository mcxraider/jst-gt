import streamlit as st
from utils.time_auth_utils import load_api_keys, save_api_keys, get_current_user, generate_dynamic_password
from datetime import datetime
import secrets
import string

def generate_api_key() -> str:
    """Generate a new API key"""
    prefix = "sk-jst-"
    suffix = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(20))
    return prefix + suffix

def api_key_management_page():
    """Admin page for managing API keys"""
    current_user = get_current_user()

    if not current_user or current_user.get("role") != "admin":
        st.error("❌ Access denied. Admin privileges required.")
        return

    st.header("🔑 API Key Management")

    api_keys = load_api_keys()

    # Display existing API keys
    st.subheader("Existing API Keys")
    for username, user_data in api_keys.items():
        with st.expander(f"👤 {username} ({user_data.get('role', 'user')})"):
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**API Key:** `{user_data['api_key']}`")
                st.write(f"**Email:** {user_data.get('email', 'N/A')}")
                st.write(f"**Created:** {user_data.get('created_at', 'N/A')}")
            with col2:
                st.write("**Current Valid Password:**")
                current_pwd = generate_dynamic_password(user_data['api_key'])
                st.code(current_pwd)

                if st.button(f"🗑️ Delete {username}", key=f"delete_{username}"):
                    if username != "user1":  # Prevent deleting default user
                        del api_keys[username]
                        save_api_keys(api_keys)
                        st.success(f"User {username} deleted!")
                        st.rerun()
                    else:
                        st.error("Cannot delete default user!")

    # Add new API key
    st.subheader("Add New User")
    with st.form("add_api_key_form"):
        new_username = st.text_input("Username")

        col1, col2 = st.columns(2)
        with col1:
            auto_generate = st.checkbox("Auto-generate API key", value=True)
        with col2:
            new_role = st.selectbox("Role", ["user", "admin"])

        if auto_generate:
            new_api_key = generate_api_key()
            st.code(f"Generated API Key: {new_api_key}")
        else:
            new_api_key = st.text_input("Custom API Key", placeholder="sk-custom-...")

        new_email = st.text_input("Email")

        if st.form_submit_button("Add User"):
            if new_username and new_api_key:
                if new_username not in api_keys:
                    api_keys[new_username] = {
                        "api_key": new_api_key,
                        "role": new_role,
                        "email": new_email,
                        "created_at": datetime.now().isoformat()
                    }
                    save_api_keys(api_keys)
                    st.success(f"User {new_username} added successfully!")
                    st.info(f"Their API key is: `{new_api_key}`")
                    st.rerun()
                else:
                    st.error("Username already exists!")
            else:
                st.error("Please fill in username and API key!")

    # System info
    st.markdown("---")
    st.subheader("System Information")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Users", len(api_keys))
    with col2:
        st.metric("Password Validity", "5 minutes")
    with col3:
        st.metric("Current Time Block", datetime.now().strftime("%H:%M"))
