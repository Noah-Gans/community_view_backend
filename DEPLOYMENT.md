# Community View Backend Deployment Guide

## Overview

This document outlines how to deploy and manage the Community View backend system on a VM. The system includes:

- **Search API**: FastAPI service for property search
- **Tegola Server**: Map tile server
- **Data Pipeline**: Automated county data processing
- **Monitoring**: Health checks and email notifications

## Prerequisites

### System Requirements
- Ubuntu 20.04+ or similar Linux distribution
- Python 3.11+
- PostgreSQL 12+
- Tegola binary installed
- 4GB+ RAM, 50GB+ storage
- Internet connectivity for data downloads

### Required Packages
```bash
sudo apt update
sudo apt install -y python3.11 python3.11-venv python3.11-dev
sudo apt install -y postgresql postgresql-contrib postgis
sudo apt install -y curl wget git build-essential
```

### Install Tegola
```bash
wget https://github.com/go-spatial/tegola/releases/download/v0.17.0/tegola_linux_amd64
sudo mv tegola_linux_amd64 /usr/local/bin/tegola
sudo chmod +x /usr/local/bin/tegola
```

## Initial Setup

### 1. Clone and Setup Repository
```bash
cd /home/ubuntu
git clone <your-repo-url> comunity_view_backend
cd comunity_view_backend

# Create and activate virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Database
```bash
# Create database and user
sudo -u postgres psql
```
```sql
CREATE DATABASE community_view;
CREATE USER community_view_user WITH PASSWORD 'secure_password';
GRANT ALL PRIVILEGES ON DATABASE community_view TO community_view_user;
\c community_view
CREATE EXTENSION postgis;
\q
```

### 3. Update Configuration
Edit `config.json` with your specific settings:
```json
{
  "general": {
    "environment": "production",
    "notification_email": "noahgans@tetoncountygis.com"
  },
  "database": {
    "host": "localhost",
    "port": 5432,
    "database": "community_view",
    "user": "community_view_user",
    "password": "secure_password"
  },
  "gcs": {
    "bucket_name": "your-actual-bucket-name",
    "upload_enabled": true
  }
}
```

### 4. Setup Google Cloud Authentication
```bash
# Install Google Cloud SDK and authenticate
curl https://sdk.cloud.google.com | bash
exec -l $SHELL
gcloud auth application-default login
```

### 5. Create Initial Search Index
```bash
source venv/bin/activate
python search_api/search_file_generator.py
```

## Service Management

### Manual Commands
```bash
# Start services
python community_view_manager.py start

# Check status
python community_view_manager.py status

# Stop services
python community_view_manager.py stop

# Run data update manually
python community_view_manager.py update

# Run health checks
python community_view_manager.py health
```

### Systemd Service (Recommended for Production)

1. **Install service file:**
```bash
sudo cp scripts/community-view.service /etc/systemd/system/
sudo systemctl daemon-reload
```

2. **Enable and start service:**
```bash
sudo systemctl enable community-view
sudo systemctl start community-view
```

3. **Monitor service:**
```bash
# Check status
sudo systemctl status community-view

# View logs
sudo journalctl -u community-view -f

# Restart service
sudo systemctl restart community-view
```

## Automated Operations

### Daily Data Updates
The system automatically runs daily at 2:00 AM (configurable in `config.json`):

1. **Download** county data for all configured counties
2. **Process** and standardize the data  
3. **Upload** to Google Cloud Storage
4. **Migrate** data to PostgreSQL database
5. **Rebuild** search index
6. **Reload** search API
7. **Send** email notification with results

### Health Monitoring
- **Health checks** run every 15 minutes
- **Email alerts** sent only when issues are detected
- **Service status** monitored (Search API, Tegola, Database)

## Endpoints

### Search API
- **Search**: `GET http://localhost:8000/search?q=query&counties=County1,County2`
- **Health**: `GET http://localhost:8000/health`
- **Docs**: `GET http://localhost:8000/docs`
- **Reload**: `POST http://localhost:8000/internal/reload-search-index` (internal)

### Tegola Server
- **Maps**: `GET http://localhost:8080/maps`
- **Tiles**: `GET http://localhost:8080/maps/your-map/{z}/{x}/{y}.pbf`

## Monitoring and Troubleshooting

### Log Files
```bash
# Application logs
tail -f logs/community_view_manager_$(date +%Y%m%d).log

# System service logs
sudo journalctl -u community-view -f

# Search API logs (when run manually)
tail -f logs/search_api.log
```

### Common Issues

1. **Service won't start:**
   - Check virtual environment: `which python` should show venv path
   - Verify dependencies: `pip list`
   - Check config file: `python -c "import json; json.load(open('config.json'))"`

2. **Database connection failed:**
   - Test connection: `psql -h localhost -U community_view_user -d community_view`
   - Check PostgreSQL service: `sudo systemctl status postgresql`

3. **Search API returns empty results:**
   - Check search index exists: `ls -la search_api/search_index.json`
   - Regenerate index: `python search_api/search_file_generator.py`
   - Reload API: `curl -X POST http://localhost:8000/internal/reload-search-index`

4. **Data update failures:**
   - Check county data sources are accessible
   - Verify GCS authentication and permissions
   - Review error logs for specific county issues

### Performance Tuning

1. **PostgreSQL optimization:**
```sql
-- Adjust these in postgresql.conf
shared_buffers = 256MB
effective_cache_size = 1GB
work_mem = 16MB
```

2. **Search API optimization:**
   - Monitor memory usage during search operations
   - Consider index partitioning for very large datasets

## Email Notifications

Configure SMTP settings in the `send_notification_email` method of `community_view_manager.py`:

```python
# Example SMTP configuration
server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login('your-email@gmail.com', 'your-app-password')
server.send_message(msg)
server.quit()
```

## Security Considerations

1. **Database**: Use strong passwords and restrict network access
2. **API**: Consider adding authentication for production use
3. **Firewall**: Only expose necessary ports (8000, 8080)
4. **Logs**: Ensure log files don't contain sensitive information
5. **GCS**: Use service accounts with minimal required permissions

## Backup Strategy

1. **Database backups:**
```bash
# Daily database backup
pg_dump -h localhost -U community_view_user community_view > backup_$(date +%Y%m%d).sql
```

2. **Configuration backups:**
```bash
# Backup critical files
tar -czf backup_config_$(date +%Y%m%d).tar.gz config.json logs/ search_api/search_index.json
```

## Scaling Considerations

For high-traffic deployments:

1. **Load balancing**: Run multiple Search API instances behind nginx
2. **Database**: Consider PostgreSQL clustering or read replicas
3. **Caching**: Add Redis for frequently accessed search results
4. **CDN**: Use cloud CDN for Tegola tile serving

## Support

For issues or questions:
- Check logs first: `sudo journalctl -u community-view -f`
- Test individual components: `python community_view_manager.py health`
- Email alerts will notify of system issues automatically 