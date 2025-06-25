"""Guest access component for login page"""
import streamlit as st

def guest_access():
    """Render the guest access button if guest mode is enabled"""
    if st.button("Continue as Guest"):
        st.session_state['guest_mode'] = True
        st.session_state['authenticated'] = True
        st.session_state['page'] = 'app'
        st.rerun()
