"""AWS OpenSearch Connector - Connect to AWS OpenSearch Service clusters."""

__version__ = "0.1.0"
__author__ = "Praveen - Claude AI"

from .client import OpenSearchClient
from .exceptions import (
    OpenSearchConnectionError,
    OpenSearchAuthError,
    OpenSearchQueryError
)

__all__ = [
    'OpenSearchClient',
    'OpenSearchConnectionError',
    'OpenSearchAuthError',
    'OpenSearchQueryError'
]