import os
from aws_opensearch_connector import OpenSearchClient

# Best practice: Load credentials from environment variables
username = os.environ.get('OPENSEARCH_USERNAME', 'admin')
password = os.environ.get('OPENSEARCH_PASSWORD')

# Initialize client
client = OpenSearchClient(
    endpoint='search-my-domain.us-east-1.es.amazonaws.com',
    username=username,
    password=password
)

# Test connection
print("Testing connection...")
if client.ping():
    print("✓ Connected successfully!")

    # Get cluster info
    info = client.get_cluster_info()
    print(f"✓ Cluster version: {info['version']['number']}")

# Create index with mapping
print("\nCreating index...")
index_body = {
    'mappings': {
        'properties': {
            'title': {'type': 'text'},
            'content': {'type': 'text'},
            'timestamp': {'type': 'date'}
        }
    }
}
client.create_index('articles', index_body)
print("✓ Index created")

# Index documents
print("\nIndexing documents...")
docs = [
    {'title': 'Python Tutorial', 'content': 'Learn Python basics'},
    {'title': 'AWS Guide', 'content': 'Getting started with AWS'},
    {'title': 'OpenSearch Deep Dive', 'content': 'Advanced OpenSearch features'}
]

for doc in docs:
    client.index_document('articles', doc)
print(f"✓ Indexed {len(docs)} documents")

# Search
print("\nSearching...")
query = {
    'query': {
        'match': {
            'content': 'AWS'
        }
    }
}
results = client.search('articles', query)
print(f"✓ Found {results['hits']['total']['value']} results")

for hit in results['hits']['hits']:
    print(f"  - {hit['_source']['title']}")

# Clean up
client.close()
print("\n✓ Connection closed")