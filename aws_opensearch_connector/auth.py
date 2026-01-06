"""Authentication utilities."""

from typing import Tuple
from .exceptions import OpenSearchAuthError


class BasicAuthProvider:
    """Provides HTTP Basic authentication for OpenSearch."""

    def __init__(self, username: str, password: str):
        """
        Initialize Basic Auth provider.

        Args:
            username: OpenSearch master username
            password: OpenSearch master password
        """
        if not username or not password:
            raise OpenSearchAuthError("Username and password are required")

        self.username = username
        self.password = password

    def get_auth(self) -> Tuple[str, str]:
        """
        Get HTTP Basic Auth tuple.

        Returns:
            Tuple of (username, password) for HTTP authentication
        """
        return (self.username, self.password)

