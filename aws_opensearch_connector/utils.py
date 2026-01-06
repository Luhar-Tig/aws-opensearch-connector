"""Utility functions."""

import re


def validate_endpoint(endpoint: str) -> str:
    """
    Validate and clean OpenSearch endpoint.

    Args:
        endpoint: OpenSearch endpoint URL

    Returns:
        Cleaned endpoint hostname
    """
    # Remove protocol if present
    endpoint = re.sub(r'^https?://', '', endpoint)

    # Remove trailing slash
    endpoint = endpoint.rstrip('/')

    # Remove port if present
    endpoint = re.sub(r':443$', '', endpoint)

    return endpoint

