# src/utils/health_check.py
from openai import OpenAI
import os
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


def check_openai_api_health():
    """
    Performs a simple health check of the OpenAI API.
    Displays the status in Streamlit.
    """
    # This is the health check function.
    # To disable it, simply comment out the call to this function in `login_page.py`.
    st.write("Performing OpenAI API health check...")
    try:
        api_key = os.getenv("API_KEY")
        if not api_key:
            st.error("OpenAI API health check failed: `API_KEY` environment variable is not set.")
            return

        client = OpenAI(api_key=api_key, base_url="https://ai-api.analytics.gov.sg")

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": "Hello"}],
            max_tokens=5,
            timeout=10,
        )

        if response.choices and response.choices[0].message.content:
            st.success("OpenAI API connection successful.")
        else:
            st.error("OpenAI API health check failed: Received an empty response.")
    except Exception as e:
        st.error(f"OpenAI API health check failed: {e}")

