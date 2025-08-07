# PostGIS + Tegola Vector Tile Pipeline

This setup replaces the tile generation and cloud upload pipeline with a PostGIS database and Tegola vector tile server.

## Benefits

- **No pre-generated tiles**: Tiles are generated on-demand
- **Real-time updates**: Database changes are immediately reflected in tiles
- **Lower storage costs**: No need to store millions of tile files
- **Efficient updates**: Only changed data needs to be updated

## Architecture

```
GeoJSON Files → PostGIS Database → Tegola Server → Vector Tiles
```

## Setup Instructions

### 1. Local Development Setup

```bash
# Install dependencies
pip install -r requirements_postgis.txt

# Install Tegola (macOS)
brew install tegola

# Install PostgreSQL and PostGIS (macOS)
brew install postgresql postgis
brew services start postgresql
```

### 2. Initialize Database

```python
from postgis_pipeline import PostGISPipeline

# Initialize pipeline
pipeline = PostGISPipeline()

# Setup database and tables
pipeline.setup_database()
```

### 3. Import Data

```python
# Import county data
pipeline.import_county_data(
    'geojson_files/teton_county_wy_ownership_parcel_address.geojson',
    'Teton County, WY'
)

# Or import all counties at once
counties_data = [
    {
        'geojson_path': 'geojson_files/teton_county_wy_ownership_parcel_address.geojson',
        'county_name': 'Teton County, WY'
    },
    # ... more counties
]
pipeline.import_all_counties(counties_data)
```

### 4. Start Tegola Server

```bash
# Start Tegola with configuration
tegola serve --config tegola_config.toml
```

### 5. Test Vector Tiles

- **Capabilities**: http://localhost:8080/maps/parcel_map/capabilities.json
- **Sample tile**: http://localhost:8080/maps/parcel_map/10/195/196.pbf

## Google Cloud VM Deployment

### 1. Create VM Instance

```bash
gcloud compute instances create parcel-tile-server \
    --zone=us-central1-a \
    --machine-type=e2-standard-4 \
    --image-family=ubuntu-2004-lts \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB
```

### 2. Setup VM

```bash
# Copy setup script to VM
gcloud compute scp setup_gcp_vm.sh ubuntu@parcel-tile-server:/home/ubuntu/

# SSH into VM
gcloud compute ssh ubuntu@parcel-tile-server

# Run setup script
chmod +x setup_gcp_vm.sh
./setup_gcp_vm.sh
```

### 3. Copy Application Files

```bash
# Copy Python files
gcloud compute scp postgis_pipeline.py ubuntu@parcel-tile-server:/opt/parcel-app/
gcloud compute scp database_manager.py ubuntu@parcel-tile-server:/opt/parcel-app/
gcloud compute scp tegola_config.toml ubuntu@parcel-tile-server:/opt/parcel-app/

# Copy GeoJSON files
gcloud compute scp --recurse geojson_files/ ubuntu@parcel-tile-server:/opt/parcel-app/
```

### 4. Import Data and Start Services

```bash
# SSH into VM
gcloud compute ssh ubuntu@parcel-tile-server

# Import data
cd /opt/parcel-app
python3 -c "
from postgis_pipeline import PostGISPipeline
pipeline = PostGISPipeline()
pipeline.setup_database()
pipeline.import_all_counties([
    {'geojson_path': 'geojson_files/teton_county_wy_ownership_parcel_address.geojson', 'county_name': 'Teton County, WY'},
    # ... add more counties
])
"

# Start Tegola service
sudo systemctl start tegola
sudo systemctl status tegola
```

## Configuration

### Database Configuration

Update `tegola_config.toml` with your database settings:

```toml
[[providers]]
name = "postgis"
type = "postgis"
host = "localhost"  # Change to VM IP for remote access
port = 5432
database = "parcel_data"
user = "postgres"
password = "your_secure_password"
```

### Tegola Configuration

The `tegola_config.toml` file defines:
- Database connection
- Layer queries
- Map configurations
- Zoom levels

## API Endpoints

### Vector Tiles
- **Base URL**: `http://YOUR_VM_IP:8080/maps/parcel_map/{z}/{x}/{y}.pbf`
- **Capabilities**: `http://YOUR_VM_IP:8080/maps/parcel_map/capabilities.json`

### Example Usage in Web Application

```javascript
// MapLibre GL JS
const map = new maplibregl.Map({
    container: 'map',
    style: {
        version: 8,
        sources: {
            'parcels': {
                type: 'vector',
                tiles: ['http://YOUR_VM_IP:8080/maps/parcel_map/{z}/{x}/{y}.pbf'],
                maxzoom: 18
            }
        },
        layers: [
            {
                id: 'parcels-fill',
                type: 'fill',
                source: 'parcels',
                'source-layer': 'parcels',
                paint: {
                    'fill-color': '#088',
                    'fill-opacity': 0.8
                }
            }
        ]
    },
    center: [-110.7619, 43.4799], // Jackson, WY
    zoom: 10
});
```

## Updating Data

### Daily Updates

```python
from postgis_pipeline import PostGISPipeline

pipeline = PostGISPipeline()

# Update specific county
pipeline.import_county_data(
    'updated_teton_county.geojson',
    'Teton County, WY'
)
```

### Automated Updates

Create a cron job or scheduled task to run your data pipeline and update the database:

```bash
# Example cron job (daily at 2 AM)
0 2 * * * cd /opt/parcel-app && python3 update_data.py
```

## Monitoring

### Check Tegola Status
```bash
sudo systemctl status tegola
```

### View Logs
```bash
sudo journalctl -u tegola -f
```

### Database Statistics
```python
from postgis_pipeline import PostGISPipeline
pipeline = PostGISPipeline()
pipeline.print_database_stats()
```

## Troubleshooting

### Common Issues

1. **Tegola won't start**: Check database connection in `tegola_config.toml`
2. **No tiles served**: Verify PostGIS extension is enabled
3. **Slow tile generation**: Add spatial indexes to your tables
4. **Connection refused**: Check firewall settings on port 8080

### Performance Optimization

1. **Add spatial indexes**:
```sql
CREATE INDEX idx_parcels_geometry ON parcels USING GIST (geometry);
```

2. **Optimize queries** in `tegola_config.toml`:
```sql
SELECT id, parcel_id, owner_name, geometry
FROM parcels
WHERE ST_Intersects(geometry, ST_Transform(!BBOX!, 4326))
AND zoom_level >= 10  -- Add zoom-based filtering
```

3. **Enable caching** in Tegola config:
```toml
[cache]
type = "file"
basepath = "/tmp/tegola_cache"
```

## Migration from Old Pipeline

Run the migration script to move from tile generation to PostGIS:

```bash
python3 migrate_to_postgis.py
```

This will:
1. Setup PostGIS database
2. Import existing GeoJSON data
3. Configure Tegola
4. Provide next steps for deployment 