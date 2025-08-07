#!/bin/bash

# Google Cloud VM Setup Script for PostGIS + Tegola
# Run this script on your Google Cloud VM instance

set -e

echo "=== Setting up Google Cloud VM for PostGIS + Tegola ==="

# Update system
echo "Updating system packages..."
sudo apt-get update
sudo apt-get upgrade -y

# Install PostgreSQL and PostGIS
echo "Installing PostgreSQL and PostGIS..."
sudo apt-get install -y postgresql postgresql-contrib postgis postgresql-14-postgis-3

# Start and enable PostgreSQL service
echo "Starting PostgreSQL service..."
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create postgres user and set password
echo "Setting up PostgreSQL user..."
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'your_secure_password';"

# Install Go (required for Tegola)
echo "Installing Go..."
wget https://go.dev/dl/go1.21.0.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.21.0.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc

# Install Tegola
echo "Installing Tegola..."
go install github.com/go-spatial/tegola/cmd/tegola@latest

# Create application directory
echo "Creating application directory..."
mkdir -p /opt/parcel-app
cd /opt/parcel-app

# Create systemd service for Tegola
echo "Creating Tegola systemd service..."
sudo tee /etc/systemd/system/tegola.service > /dev/null <<EOF
[Unit]
Description=Tegola Vector Tile Server
After=network.target postgresql.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/opt/parcel-app
ExecStart=/home/ubuntu/go/bin/tegola serve --config /opt/parcel-app/tegola_config.toml
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Enable and start Tegola service
sudo systemctl daemon-reload
sudo systemctl enable tegola

# Configure firewall
echo "Configuring firewall..."
sudo ufw allow 22/tcp
sudo ufw allow 5432/tcp
sudo ufw allow 8080/tcp
sudo ufw --force enable

# Install Python and required packages
echo "Installing Python and required packages..."
sudo apt-get install -y python3 python3-pip python3-venv
pip3 install psycopg2-binary geopandas

# Create a simple startup script
cat > /opt/parcel-app/start.sh << 'EOF'
#!/bin/bash
cd /opt/parcel-app

# Start Tegola if not running
if ! systemctl is-active --quiet tegola; then
    echo "Starting Tegola service..."
    sudo systemctl start tegola
fi

echo "PostGIS + Tegola setup complete!"
echo "Tegola is running on http://$(curl -s ifconfig.me):8080"
echo "Database is accessible on localhost:5432"
EOF

chmod +x /opt/parcel-app/start.sh

echo "=== Setup Complete! ==="
echo ""
echo "Next steps:"
echo "1. Copy your Python files to /opt/parcel-app/"
echo "2. Update tegola_config.toml with your database settings"
echo "3. Run: sudo systemctl start tegola"
echo "4. Test: curl http://localhost:8080/maps/parcel_map/capabilities.json"
echo ""
echo "To copy files from your local machine:"
echo "gcloud compute scp --recurse ./postgis_pipeline.py ubuntu@YOUR_VM_NAME:/opt/parcel-app/"
echo "gcloud compute scp --recurse ./database_manager.py ubuntu@YOUR_VM_NAME:/opt/parcel-app/"
echo "gcloud compute scp --recurse ./tegola_config.toml ubuntu@YOUR_VM_NAME:/opt/parcel-app/"
echo ""
echo "To connect to your VM:"
echo "gcloud compute ssh ubuntu@YOUR_VM_NAME" 