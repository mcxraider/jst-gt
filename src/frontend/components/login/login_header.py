"""Login page header component"""

import streamlit as st
import sys
from pathlib import Path

# Add src directory to path for config import
sys.path.append(str(Path(__file__).parent.parent.parent))
from config import APP_NAME_DISPLAY, PAGE_ICON


def login_header():
    """Render the login page header with SAIL logo and subtitle"""
    # Get the logo path
    project_root = Path(__file__).parent.parent.parent.parent
    logo_path = project_root / PAGE_ICON.lstrip("../")

    # Create header with logo if available
    if logo_path.exists():
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            # Display logo
            st.image(str(logo_path), width=200)
            st.markdown(
                f"""
            <div style="text-align: center; margin-top: 1rem;">
                <h2 style="color: #2c3e50; margin-bottom: 1rem;">Welcome to the {APP_NAME_DISPLAY}</h2>
            </div>
            """,
                unsafe_allow_html=True,
            )
    else:
        # Fallback to text-only header
        st.markdown(
            f"""
        <div style="text-align: center; margin-bottom: 2rem;">
            <h1 style="color: #2c3e50; margin-bottom: 1rem;">Welcome to the {APP_NAME_DISPLAY}</h1>
        </div>
        """,
            unsafe_allow_html=True,
        )
