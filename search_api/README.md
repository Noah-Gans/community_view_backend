# Property Search API

A FastAPI-based search service for property ownership data across multiple counties. This API provides search functionality that matches your frontend search algorithm while keeping the search index lightweight and manageable.

## Features

- **Lightweight Search Index**: Only essential fields are included to keep file size manageable
- **Frontend-Compatible Algorithm**: Uses the same search scoring and ranking as your frontend
- **Multi-County Support**: Searches across all configured counties
- **Spatial Queries**: Includes bounding box data for map integration
- **RESTful API**: Clean, documented endpoints

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Generate the search index (first time setup):
```bash
python -m search_api.search_file_generator
```

## Running the API

### Development
```bash
uvicorn search_api.app:app --reload --host 0.0.0.0 --port 8000
```

### Production
```bash
uvicorn search_api.app:app --host 0.0.0.0 --port 8000
```

## API Endpoints

### Search Properties
```
GET /search?q={query}&limit={limit}
```

**Parameters:**
- `q` (required): Search query string
- `limit` (optional): Maximum number of results (default: 200)

**Example:**
```bash
curl "http://localhost:8000/search?q=john%20smith&limit=50"
```

### Get Statistics
```
GET /stats
```

Returns information about the search index including total entries and county breakdown.

### Health Check
```
GET /health
```

Returns API health status and search index information.

### Reload Search Index
```
POST /reload
```

Reloads the search index from file (useful after updates).

### Generate New Search Index
```
POST /generate-search-index
```

Generates a new search index from county data files.

## Search Algorithm

The search algorithm matches your frontend implementation:

1. **Exact Phrase Match** (500 points): Exact phrase found in any field
2. **All Words Match** (250 points): All query words appear as whole words
3. **Any Word Match** (100 points): At least one word is an exact match
4. **Partial Match** (10 points): Partial word matches
5. **Field Boosts**: Owner field (+100), Physical address (+50)
6. **Filtering**: Only results with score >= 300 are returned
7. **Ranking**: Results sorted by score (highest first)
8. **Limiting**: Maximum 200 results returned

## Search Index Structure

The search index contains only essential fields to keep file size manageable:

```json
{
  "pidn": "Parcel ID number",
  "owner": "Owner name",
  "address": "Mailing address",
  "st_address": "Physical address",
  "county": "County name",
  "bbox": [min_lon, min_lat, max_lon, max_lat],
  "clerk_rec": "Clerk records link",
  "property_det": "Property details link",
  "tax_info": "Tax details link"
}
```

## Configuration

### Counties Supported
- Fremont County, WY
- Teton County, ID
- Sublette County, WY
- Lincoln County, WY
- Teton County, WY

### File Paths
- Search index: `search_index.json` (in API directory)
- County data: `../tile_cycle/geojsons_for_db_upload/{county}_data_files/{county}_final_ownership.geojson`

## Daily Updates

To set up daily search index updates:

1. Create a cron job or scheduled task
2. Call the `/generate-search-index` endpoint or run the generator script
3. The API will automatically reload the new index

Example cron job (runs daily at 6 AM):
```bash
0 6 * * * curl -X POST http://localhost:8000/generate-search-index
```

## Error Handling

The API includes comprehensive error handling:
- Invalid search queries return 400
- Search failures return 500
- Missing search index shows warnings
- All errors are logged for debugging

## Performance

- Search index is loaded once at startup
- Search operations are in-memory for fast response
- Results are limited to prevent overwhelming responses
- Search timing is included in responses for monitoring 