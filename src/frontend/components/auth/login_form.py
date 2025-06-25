import streamlit as st
from utils.time_auth_utils import authenticate_user, get_next_password_time
from frontend.components.auth.password_generator import password_generator
from datetime import datetime

def login_form():
    """Render the time-based login form"""

    # Show current time and next password change
    current_time = datetime.now().strftime("%H:%M:%S")
    next_change = get_next_password_time()

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col3:
        st.metric("Current Time", current_time)
    with col4:
        st.metric("Password Changes At", next_change)

    # Login form
    with st.form("login_form"):
        username = st.text_input(
            "Username",
            placeholder="Enter your username",
            help="Your assigned username"
        )
        password = st.text_input(
            "Dynamic Password",
            type="password",
            placeholder="Enter your time-based password",
            help="Generate this using your API key + current time block"
        )

        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            submit_button = st.form_submit_button("Login", use_container_width=True)

        if submit_button:
            if username and password:
                is_valid, user_info = authenticate_user(username, password)

                if is_valid:
                    st.session_state.authenticated = True
                    st.session_state.user_info = user_info
                    st.session_state.username = username
                    st.success("✅ Login successful!")
                    st.rerun()
                else:
                    st.error("❌ Invalid username or password. Password may have expired.")
                    st.warning("💡 Passwords change every 5 minutes. Generate a new one below.")
            else:
                st.warning("⚠️ Please enter both username and password")

    st.markdown("</div>", unsafe_allow_html=True)

    # Password generator section
    st.markdown("<br>", unsafe_allow_html=True)
    password_generator()

    # Instructions
    st.markdown("---")
    st.markdown("### 📖 How It Works")
    st.markdown("""
    1. **Time-Based Security**: Passwords change every 5 minutes automatically
    2. **API Key Required**: Use your personal API key to generate current password
    3. **Dynamic Generation**: Password = Hash(API_Key + App_Name + Time_Block)
    4. **Limited Validity**: Each password is only valid for 5 minutes

    **Default Demo Credentials:**
    - Username: `user1`
    - API Key: `sk-demo-abc123xyz789`
    """)
