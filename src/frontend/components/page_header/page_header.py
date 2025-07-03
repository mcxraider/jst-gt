import streamlit as st
from datetime import datetime
from zoneinfo import ZoneInfo


# Function to create a decorative header with icon and timestamp
def create_header():
    page_title = "Skill Proficiency AI Tagger"
    sgt = ZoneInfo("Asia/Singapore")
    current_time = datetime.now(sgt).strftime("%b %d, %Y • %I:%M %p")

    col1, col2 = st.columns([3, 1])

    with col1:
        st.markdown(
            f"<h1 style='margin-bottom: 0px;'>{page_title}</h1>", unsafe_allow_html=True
        )

    with col2:
        st.markdown(
            f"<p style='text-align: right; color: #6c757d; margin-top: 12px;'>{current_time}</p>",
            unsafe_allow_html=True,
        )

    st.markdown("<hr style='margin: 10px 0px 5px 0px;'>", unsafe_allow_html=True)
