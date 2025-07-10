# src/utils/health_check.py
from openai import OpenAI
import os
import logging
from dotenv import load_dotenv

load_dotenv()

# Configure logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def check_openai_api_health():
    """
    Performs a simple health check of the OpenAI API.
    Logs the status to the console.
    """
    # This is the health check function.
    # To disable it, simply comment out the call to this function in `login_page.py`.
    logger.info("Performing OpenAI API health check...")
    try:
        api_key = os.getenv("API_KEY")
        if not api_key:
            logger.error(
                "OpenAI API health check failed: `API_KEY` environment variable is not set."
            )
            return

        client = OpenAI(api_key=api_key)

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": "Hello"}],
            max_tokens=5,
            timeout=10,
        )

        if response.choices and response.choices[0].message.content:
            logger.info("OpenAI API connection successful.")
        else:
            logger.error("OpenAI API health check failed: Received an empty response.")
    except Exception as e:
        logger.error(f"OpenAI API health check failed: {e}", exc_info=True)
