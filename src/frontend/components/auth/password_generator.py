import streamlit as st
from utils.time_auth_utils import generate_dynamic_password, get_current_time_block, get_next_password_time
from datetime import datetime

def password_generator():
    """Component to help users generate their current password"""
    st.markdown("### 🔑 Password Generator")

    with st.expander("Generate Your Current Password"):
        st.info("Enter your API key to generate the current valid password")

        api_key_input = st.text_input(
            "Your API Key",
            type="password",
            placeholder="sk-demo-abc123xyz789",
            help="This is your personal API key provided by the administrator"
        )

        if api_key_input:
            current_password = generate_dynamic_password(api_key_input)
            time_block = get_current_time_block()
            next_change = get_next_password_time()

            st.success(f"**Current Password:** `{current_password}`")
            st.info(f"**Valid until:** {next_change}")
            st.caption(f"Time block: {time_block}")

            # Auto-refresh every 30 seconds
            if st.button("🔄 Refresh Password"):
                st.rerun()
