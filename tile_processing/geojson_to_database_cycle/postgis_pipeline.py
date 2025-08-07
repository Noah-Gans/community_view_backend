import os
from geojson_to_database_cycle.database_manager import DatabaseManager

import subprocess
import json

class PostGISPipeline:
    def __init__(self, db_config=None):
        """
        Initialize the PostGIS pipeline.
        
        Args:
            db_config (dict): Database configuration with keys:
                - host: database host (default: localhost)
                - port: database port (default: 5432)
                - database: database name (default: parcel_data)
                - user: database user (default: postgres)
                - password: database password
        """
        if db_config is None:
            db_config = {
                'host': 'localhost',
                'port': 5432,
                'database': 'parcel_data',
                'user': 'postgres',
                'password': 'your_password'
            }
        
        self.db_manager = DatabaseManager(**db_config)
        self.tegola_config_path = 'tegola_config.toml'
    
    def setup_database(self):
        """Initialize database and create tables."""
        print("Setting up PostGIS database...")
        self.db_manager.create_database_and_extensions()
        self.db_manager.create_parcel_table()
        print("Database setup complete!")
    
    def import_county_data(self, geojson_file_path, county_name):
        """
        Import county parcel data into PostGIS.
        
        Args:
            geojson_file_path (str): Path to the GeoJSON file
            county_name (str): Name of the county
        """
        print(f"Importing {county_name} data...")
        
        # Check if file exists
        if not os.path.exists(geojson_file_path):
            print(f"Error: File {geojson_file_path} not found!")
            return False
        
        try:
            # Update existing data or insert new records
            self.db_manager.update_parcel_data(
                geojson_file_path, 
                table_name='parcels', 
                county_name=county_name
            )
            print(f"Successfully imported {county_name} data!")
            return True
        except Exception as e:
            print(f"Error importing {county_name} data: {e}")
            return False
    
    def import_all_counties(self, counties_data):
        """
        Import data for multiple counties.
        
        Args:
            counties_data (list): List of dicts with keys:
                - geojson_path: path to GeoJSON file
                - county_name: name of the county
        """
        print("Importing all county data...")
        
        for county in counties_data:
            success = self.import_county_data(
                county['geojson_path'], 
                county['county_name']
            )
            if not success:
                print(f"Failed to import {county['county_name']}")
        
        print("County data import complete!")
    
    def start_tegola_server(self, config_path=None):
        """
        Start the Tegola tile server.
        
        Args:
            config_path (str): Path to Tegola config file
        """
        if config_path is None:
            config_path = self.tegola_config_path
        
        if not os.path.exists(config_path):
            print(f"Error: Tegola config file {config_path} not found!")
            return False
        
        print("Starting Tegola tile server...")
        try:
            # Start Tegola server
            subprocess.run([
                'tegola', 'serve', '--config', config_path
            ], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error starting Tegola server: {e}")
            return False
        except FileNotFoundError:
            print("Error: Tegola not found. Please install Tegola first.")
            return False
        
        return True
    
    def get_tile_url(self, map_name='parcel_map'):
        """
        Get the base URL for vector tiles.
        
        Args:
            map_name (str): Name of the map from config
            
        Returns:
            str: Base URL for vector tiles
        """
        return f"http://localhost:8080/maps/{map_name}/{{z}}/{{x}}/{{y}}.pbf"
    
    def get_capabilities_url(self, map_name='parcel_map'):
        """
        Get the capabilities URL for the map.
        
        Args:
            map_name (str): Name of the map from config
            
        Returns:
            str: Capabilities URL
        """
        return f"http://localhost:8080/maps/{map_name}/capabilities.json"
    
    def update_tegola_config(self, db_config):
        """
        Update the Tegola configuration with new database settings.
        
        Args:
            db_config (dict): Database configuration
        """
        config_content = f"""# Tegola configuration file
[webserver]
port = ":8080"

[cache]
type = "file"
basepath = "/tmp/tegola_cache"

# Database connection
[[providers]]
name = "postgis"
type = "postgis"
host = "{db_config.get('host', 'localhost')}"
port = {db_config.get('port', 5432)}
database = "{db_config.get('database', 'parcel_data')}"
user = "{db_config.get('user', 'postgres')}"
password = "{db_config.get('password', 'your_password')}"
ssl_mode = "disable"

# Parcel layer configuration
[[providers.layers]]
name = "parcels"
geometry_fieldname = "geometry"
id_fieldname = "id"
sql = \"\"\"
SELECT 
    id,
    parcel_id,
    owner_name,
    address,
    county,
    state,
    properties,
    ST_AsBinary(geometry) AS geometry
FROM parcels
WHERE ST_Intersects(geometry, ST_Transform(!BBOX!, 4326))
\"\"\"

# Map configuration
[[maps]]
name = "parcel_map"
attribution = "Parcel Data"
bounds = [-180, -85, 180, 85]

[[maps.layers]]
provider_layer = "postgis.parcels"
min_zoom = 6
max_zoom = 18
"""
        
        with open(self.tegola_config_path, 'w') as f:
            f.write(config_content)
        
        print(f"Updated Tegola configuration: {self.tegola_config_path}")
    
    def get_database_stats(self):
        """Get statistics about the database."""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        
        # Get total count
        cursor.execute("SELECT COUNT(*) FROM parcels")
        total_count = cursor.fetchone()[0]
        
        # Get spatial vs non-spatial counts
        cursor.execute("""
            SELECT has_spatial_data, COUNT(*) 
            FROM parcels 
            GROUP BY has_spatial_data
        """)
        spatial_counts = cursor.fetchall()
        
        # Get count by county
        cursor.execute("""
            SELECT county, COUNT(*) 
            FROM parcels 
            GROUP BY county 
            ORDER BY county
        """)
        county_counts = cursor.fetchall()
        
        conn.close()
        
        spatial_count = 0
        non_spatial_count = 0
        for has_spatial, count in spatial_counts:
            if has_spatial:
                spatial_count = count
            else:
                non_spatial_count = count
        
        stats = {
            'total_parcels': total_count,
            'spatial_parcels': spatial_count,
            'non_spatial_parcels': non_spatial_count,
            'counties': {county: count for county, count in county_counts}
        }
        
        return stats
    
    def print_database_stats(self):
        """Print database statistics."""
        stats = self.get_database_stats()
        
        print("\n=== Database Statistics ===")
        print(f"Total parcels: {stats['total_parcels']}")
        print(f"Spatial parcels: {stats['spatial_parcels']}")
        print(f"Non-spatial parcels: {stats['non_spatial_parcels']}")
        print("\nParcels by county:")
        for county, count in stats['counties'].items():
            print(f"  {county}: {count} parcels")
        print("========================\n") 