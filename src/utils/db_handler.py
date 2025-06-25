"""Simple authentication handler for login"""


def authenticate_user(email: str, password: str) -> bool:
    """
    Simple authentication function
    Validates email ends with @ssg.gov.sg and password is 'ssgiddapp'
    """
    if not email or not password:
        return False

    # Check email domain
    if not email.endswith("@ssg.gov.sg"):
        return False

    # Check password
    if password != "ssgiddapp":
        return False

    return True
