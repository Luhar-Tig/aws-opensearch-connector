"""Custom exceptions for AWS OpenSearch connector."""


class OpenSearchConnectionError(Exception):
    """Raised when connection to OpenSearch fails."""
    pass


class OpenSearchAuthError(Exception):
    """Raised when authentication fails."""
    pass


class OpenSearchQueryError(Exception):
    """Raised when a query operation fails."""
    pass