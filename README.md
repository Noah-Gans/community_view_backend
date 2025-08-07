# Community View Backend

A comprehensive backend system for property search and map tile serving with automated data processing.

## Quick Start

### 1. Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Configure system
cp config.json.example config.json  # Edit with your settings
```

### 2. Generate Initial Search Index
```bash
python search_api/search_file_generator.py
```

### 3. Start Services
```bash
# Manual start
python community_view_manager.py start

# Or start as daemon with scheduling
python community_view_manager.py daemon
```

## Management Commands

```bash
# Service management
python community_view_manager.py start     # Start all services
python community_view_manager.py stop      # Stop all services
python community_view_manager.py status    # Check service status

# Operations
python community_view_manager.py update    # Run data update cycle
python community_view_manager.py health    # Run health checks
python community_view_manager.py daemon    # Run with scheduling

# Individual services (alternative)
./scripts/start_services.sh                # Start services manually
./scripts/stop_services.sh                 # Stop services manually
```

## API Endpoints

### Search API (Port 8000)
- **Search**: `GET /search?q=query&counties=County1,County2&limit=100`
- **Health**: `GET /health`
- **Documentation**: `GET /docs`

### Tegola Server (Port 8080)
- **Maps**: `GET /maps`
- **Tiles**: `GET /maps/{map_name}/{z}/{x}/{y}.pbf`

## Features

- ✅ **Property Search**: Fast search across 93k+ properties with county filtering
- ✅ **Automated Data Pipeline**: Daily updates at 2 AM with GCS upload and PostgreSQL migration
- ✅ **Health Monitoring**: 15-minute health checks with email notifications
- ✅ **Map Tile Serving**: Tegola-based tile server for geospatial data
- ✅ **Service Management**: Comprehensive start/stop/status management
- ✅ **Production Ready**: Systemd service, logging, error handling

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Search API    │    │  Tegola Server   │    │ Management      │
│   (Port 8000)   │    │   (Port 8080)    │    │ Daemon          │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                        │                       │
         └────────────────────────┼───────────────────────┘
                                  │
         ┌────────────────────────┼───────────────────────┐
         │                       │                       │
   ┌──────────┐         ┌─────────────┐         ┌─────────────┐
   │PostgreSQL│         │    GCS      │         │   County    │
   │Database  │         │   Bucket    │         │Data Sources │
   └──────────┘         └─────────────┘         └─────────────┘
```

## Daily Automated Workflow

1. **02:00 AM**: Data update cycle begins
2. **Download**: Fetch latest county data for all configured counties
3. **Process**: Standardize and validate geospatial data
4. **Upload**: Store processed files in Google Cloud Storage
5. **Database**: Migrate data to PostgreSQL with PostGIS
6. **Search Index**: Rebuild optimized search index (93k+ entries)
7. **Reload**: Hot-reload search API with new data
8. **Notify**: Email summary to `noahgans@tetoncountygis.com`

## Configuration

Edit `config.json` for your environment:

```json
{
  "general": {
    "notification_email": "your-email@domain.com"
  },
  "counties": [
    "fremont_county_wy",
    "teton_county_id", 
    "lincoln_county_wy",
    "sublette_county_wy",
    "teton_county_wy"
  ],
  "database": {
    "host": "localhost",
    "database": "community_view",
    "user": "your_db_user"
  }
}
```

## Production Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md) for complete VM deployment instructions.

```bash
# Install as systemd service
sudo cp scripts/community-view.service /etc/systemd/system/
sudo systemctl enable community-view
sudo systemctl start community-view
```

## Monitoring

- **Logs**: `tail -f logs/community_view_manager_$(date +%Y%m%d).log`
- **Service Status**: `sudo systemctl status community-view`
- **Health Dashboard**: `curl http://localhost:8000/health`

## Support

For issues or questions, check:
1. Service status: `python community_view_manager.py status`
2. Health checks: `python community_view_manager.py health`
3. Log files in `logs/` directory
4. Email notifications (sent automatically on errors) 