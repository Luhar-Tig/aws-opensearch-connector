from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from opensearchpy import OpenSearch
import csv
import io
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
import os
from pathlib import Path

app = FastAPI(title="OpenSearch Query Application")

# Get the directory where main.py is located
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# OpenSearch Configuration
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "your-opensearch-endpoint.com")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", "9200"))
OPENSEARCH_INDEX = os.getenv("OPENSEARCH_INDEX", "your-target-index")
OPENSEARCH_USERNAME = os.getenv("OPENSEARCH_USERNAME", "admin")
OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD", "your-password")
OPENSEARCH_USE_SSL = os.getenv("OPENSEARCH_USE_SSL", "true").lower() == "true"
OPENSEARCH_VERIFY_CERTS = os.getenv("OPENSEARCH_VERIFY_CERTS", "true").lower() == "true"

# Initialize OpenSearch client with username/password authentication
opensearch_client = OpenSearch(
    hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
    http_auth=(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD),
    use_ssl=OPENSEARCH_USE_SSL,
    verify_certs=OPENSEARCH_VERIFY_CERTS,
    ssl_show_warn=False
)


class SearchParams(BaseModel):
    region: str = "A"
    entity_name: Optional[str] = None
    business_area: str = "A"
    data_source: Optional[str] = None
    page: int = 1
    page_size: int = 100  # Changed from 10 to 100


def build_opensearch_query(params: SearchParams) -> Dict[str, Any]:
    """Build OpenSearch query with nested structure support"""
    must_clauses = []

    # Add region filter
    must_clauses.append({
        "match": {
            "region": params.region
        }
    })

    # Add business area filter
    must_clauses.append({
        "match": {
            "business_area": params.business_area
        }
    })

    # Add entity name filter if provided
    if params.entity_name and params.entity_name.strip():
        must_clauses.append({
            "match": {
                "entity_name": params.entity_name
            }
        })

    # Add data source filter if provided
    if params.data_source and params.data_source.strip():
        must_clauses.append({
            "match": {
                "data_source": params.data_source
            }
        })

    # Build the query
    query = {
        "query": {
            "bool": {
                "must": must_clauses
            }
        },
        "from": (params.page - 1) * params.page_size,
        "size": params.page_size
    }

    return query


def search_opensearch(params: SearchParams) -> Dict[str, Any]:
    """Execute search on OpenSearch"""
    try:
        query = build_opensearch_query(params)
        response = opensearch_client.search(
            index=OPENSEARCH_INDEX,
            body=query,
            request_timeout=300  # 5 minute timeout
        )

        # Handle different total hit formats (OpenSearch 1.x vs 2.x)
        total_hits = response['hits']['total']
        if isinstance(total_hits, dict):
            total_hits = total_hits.get('value', 0)

        hits = response['hits']['hits']

        results = []
        for hit in hits:
            results.append(hit['_source'])

        return {
            "total": total_hits,
            "results": results,
            "page": params.page,
            "page_size": params.page_size,
            "total_pages": (total_hits + params.page_size - 1) // params.page_size
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OpenSearch error: {str(e)}")


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Render the main search page"""
    try:
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "regions": ["A", "E", "I", "O", "U"],
                "business_areas": ["A", "E", "I", "O", "U"]
            }
        )
    except Exception as e:
        return HTMLResponse(
            content=f"<h1>Error loading template</h1><p>{str(e)}</p><p>Make sure 'templates/index.html' exists in the same directory as main.py</p>",
            status_code=500
        )


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "templates_dir": str(BASE_DIR / "templates")}


@app.get("/search")
async def search(
        region: str = Query("A"),
        entity_name: Optional[str] = Query(None, max_length=100),
        business_area: str = Query("A"),
        data_source: Optional[str] = Query(None, max_length=100),
        page: int = Query(1, ge=1)
):
    """Execute search and return results with limited columns for UI"""
    params = SearchParams(
        region=region,
        entity_name=entity_name,
        business_area=business_area,
        data_source=data_source,
        page=page
    )

    results = search_opensearch(params)

    # Column names to display
    ui_columns = ['tradeID', 'tradeIdInternal', 'primaryAssetClass', 'sourceSystemName', 'tradeDate']

    limited_results = []

    for record in results['results']:
        limited_record = {}
        for col in ui_columns:
            value = get_nested_value(record, col)

            # Convert epoch to DD-MON-YYYY UTC for tradeDate
            if col == 'tradeDate' and value:
                value = format_epoch_to_date(value)

            limited_record[col] = value
        limited_results.append(limited_record)

    results['results'] = limited_results
    return results


def format_epoch_to_date(epoch_value: Any) -> str:
    """Convert epoch timestamp to DD-MON-YYYY UTC format"""
    try:
        # Handle both seconds and milliseconds timestamps
        timestamp = float(epoch_value)
        if timestamp > 10000000000:  # Likely milliseconds
            timestamp = timestamp / 1000

        from datetime import datetime
        dt = datetime.utcfromtimestamp(timestamp)
        return dt.strftime('%d-%b-%Y UTC').upper()
    except (ValueError, TypeError):
        return str(epoch_value)  # Return as-is if conversion fails


def get_nested_value(data: Dict[str, Any], key: str) -> Any:
    """Get value from nested dict using dot notation (e.g., 'parent.child')"""
    keys = key.split('.')
    value = data
    for k in keys:
        if isinstance(value, dict):
            value = value.get(k, '')
        else:
            return ''
    return value if value is not None else ''


@app.get("/export")
async def export_csv(
        region: str = Query("A"),
        entity_name: Optional[str] = Query(None, max_length=100),
        business_area: str = Query("A"),
        data_source: Optional[str] = Query(None, max_length=100)
):
    """Export search results to CSV (max 100 records)"""
    params = SearchParams(
        region=region,
        entity_name=entity_name,
        business_area=business_area,
        data_source=data_source,
        page=1,
        page_size=100
    )

    results = search_opensearch(params)

    # Create CSV in memory
    output = io.StringIO()

    if results['results']:
        # Extract all unique keys from results
        all_keys = set()
        for record in results['results']:
            all_keys.update(flatten_dict(record).keys())

        fieldnames = sorted(list(all_keys))
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for record in results['results']:
            flattened = flatten_dict(record)
            writer.writerow(flattened)

    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=opensearch_results.csv"
        }
    )


def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
    """Flatten nested dictionary for CSV export"""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, str(v)))
        else:
            items.append((new_key, v))
    return dict(items)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)