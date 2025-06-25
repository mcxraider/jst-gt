"""Authentication handler for login"""
from utils.time_auth_utils import authenticate_user as auth_user


def authenticate_user(email: str, password: str) -> bool:
    """
    Authentication function that uses time-based password authentication
    """
    if not email or not password:
        return False

    # Use the time-based authentication from time_auth_utils
    is_valid, user_data = auth_user(email, password)
    return is_valid
