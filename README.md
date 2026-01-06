# AWS OpenSearch Connector

A Python package for easily connecting to AWS OpenSearch Service clusters with IAM authentication.

## Features

- Simple, intuitive API for OpenSearch operations
- Username/password (HTTP Basic) authentication
- Support for all standard OpenSearch operations (index, search, bulk operations)
- Comprehensive error handling
- Type hints for better IDE support

## Installation

```bash
pip install aws-opensearch-connector
```

## Quick Start

```python
from aws_opensearch_connector import OpenSearchClient

# Initialize client with username/password
client = OpenSearchClient(
    endpoint='search-my-domain.us-east-1.es.amazonaws.com',
    username='admin',
    password='YourStrongPassword123!'
)

# Test connection
if client.ping():
    print("Connected successfully!")

# Create an index
client.create_index('my-index')

# Index a document
doc = {'title': 'Hello', 'content': 'World'}
client.index_document('my-index', doc)

# Search
query = {
    'query': {
        'match': {
            'title': 'Hello'
        }
    }
}
results = client.search('my-index', query)
print(results)
```

## Configuration Options

```python
client = OpenSearchClient(
    endpoint='search-my-domain.us-east-1.es.amazonaws.com',
    username='admin',
    password='YourStrongPassword123!',
    port=443,  # Default: 443
    use_ssl=True,  # Default: True
    verify_certs=True,  # Default: True
    timeout=30,  # Default: 30 seconds
    ca_certs='/path/to/ca-bundle.crt'  # Optional: Custom CA certificate
)
```

## API Reference

### OpenSearchClient

Main client class for interacting with OpenSearch.

**Methods:**
- `ping()` - Test connection
- `get_cluster_info()` - Get cluster information
- `create_index(index_name, body=None)` - Create an index
- `delete_index(index_name)` - Delete an index
- `index_document(index_name, document, doc_id=None)` - Index a document
- `get_document(index_name, doc_id)` - Get a document by ID
- `search(index_name, query)` - Execute a search query
- `bulk_index(index_name, documents)` - Bulk index documents
- `close()` - Close the connection

## Requirements

- Python 3.8+
- opensearch-py

## Security Best Practices

- Never hardcode credentials in your source code
- Use environment variables or secure configuration management
- Enable SSL/TLS (use_ssl=True) for production
- Verify certificates (verify_certs=True) for production
- Use strong passwords for OpenSearch master user
- Rotate credentials regularly

## License

MIT License
