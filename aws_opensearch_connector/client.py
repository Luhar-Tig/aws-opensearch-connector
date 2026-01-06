"""Main client for AWS OpenSearch connections."""

from typing import Dict, List, Optional, Any
from opensearchpy import OpenSearch, RequestsHttpConnection
from .auth import BasicAuthProvider
from .exceptions import OpenSearchConnectionError, OpenSearchQueryError
from .utils import validate_endpoint


class OpenSearchClient:
    """Client for connecting to AWS OpenSearch Service."""

    def __init__(
            self,
            endpoint: str,
            username: str,
            password: str,
            port: int = 443,
            use_ssl: bool = True,
            verify_certs: bool = True,
            timeout: int = 30,
            ca_certs: Optional[str] = None
    ):
        """
        Initialize OpenSearch client with username/password authentication.

        Args:
            endpoint: OpenSearch cluster endpoint (e.g., 'search-domain.region.es.amazonaws.com')
            username: Master username for OpenSearch cluster
            password: Master password for OpenSearch cluster
            port: Port number (default: 443)
            use_ssl: Use SSL connection
            verify_certs: Verify SSL certificates
            timeout: Connection timeout in seconds
            ca_certs: Path to CA certificate file (optional)
        """
        self.endpoint = validate_endpoint(endpoint)
        self.username = username

        # Create basic auth tuple
        auth_provider = BasicAuthProvider(username, password)
        self.http_auth = auth_provider.get_auth()

        # Initialize OpenSearch client
        try:
            client_config = {
                'hosts': [{'host': self.endpoint, 'port': port}],
                'http_auth': self.http_auth,
                'use_ssl': use_ssl,
                'verify_certs': verify_certs,
                'connection_class': RequestsHttpConnection,
                'timeout': timeout
            }

            # Add CA certs if provided
            if ca_certs:
                client_config['ca_certs'] = ca_certs

            self.client = OpenSearch(**client_config)
        except Exception as e:
            raise OpenSearchConnectionError(f"Failed to initialize OpenSearch client: {str(e)}")

    def ping(self) -> bool:
        """Test connection to OpenSearch cluster."""
        try:
            return self.client.ping()
        except Exception as e:
            raise OpenSearchConnectionError(f"Failed to ping cluster: {str(e)}")

    def get_cluster_info(self) -> Dict[str, Any]:
        """Get cluster information."""
        try:
            return self.client.info()
        except Exception as e:
            raise OpenSearchConnectionError(f"Failed to get cluster info: {str(e)}")

    def create_index(self, index_name: str, body: Optional[Dict] = None) -> Dict[str, Any]:
        """Create an index."""
        try:
            return self.client.indices.create(index=index_name, body=body or {})
        except Exception as e:
            raise OpenSearchQueryError(f"Failed to create index: {str(e)}")

    def delete_index(self, index_name: str) -> Dict[str, Any]:
        """Delete an index."""
        try:
            return self.client.indices.delete(index=index_name)
        except Exception as e:
            raise OpenSearchQueryError(f"Failed to delete index: {str(e)}")

    def index_document(self, index_name: str, document: Dict, doc_id: Optional[str] = None) -> Dict[str, Any]:
        """Index a document."""
        try:
            return self.client.index(index=index_name, body=document, id=doc_id)
        except Exception as e:
            raise OpenSearchQueryError(f"Failed to index document: {str(e)}")

    def get_document(self, index_name: str, doc_id: str) -> Dict[str, Any]:
        """Get a document by ID."""
        try:
            return self.client.get(index=index_name, id=doc_id)
        except Exception as e:
            raise OpenSearchQueryError(f"Failed to get document: {str(e)}")

    def search(self, index_name: str, query: Dict) -> Dict[str, Any]:
        """Execute a search query."""
        try:
            return self.client.search(index=index_name, body=query)
        except Exception as e:
            raise OpenSearchQueryError(f"Failed to execute search: {str(e)}")

    def bulk_index(self, index_name: str, documents: List[Dict]) -> Dict[str, Any]:
        """Bulk index documents."""
        from opensearchpy import helpers

        actions = [
            {
                "_index": index_name,
                "_source": doc
            }
            for doc in documents
        ]

        try:
            return helpers.bulk(self.client, actions)
        except Exception as e:
            raise OpenSearchQueryError(f"Failed to bulk index: {str(e)}")

    def close(self):
        """Close the client connection."""
        if hasattr(self, 'client'):
            self.client.close()

