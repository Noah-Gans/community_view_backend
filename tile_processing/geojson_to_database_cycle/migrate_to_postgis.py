#!/usr/bin/env python3
"""
Migration script to move from tile generation + cloud upload to PostGIS + Tegola
"""

import os
import json
import argparse
import sys
from pathlib import Path
from geojson_to_database_cycle.postgis_pipeline import PostGISPipeline

def get_county_data_files():
    """Define all available county data files with proper path resolution"""
    # Get the script's directory and resolve paths relative to the project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent  # Go up one level from geojson_to_database_cycle
    
    return {
        'teton_county_wy': {
            'geojson_path': str(project_root / 'geojsons_for_db_upload' / 'teton_county_wy_data_files' / 'teton_county_wy_final_ownership.geojson'),
            'county_name': 'Teton County, WY'
        },
        'sublette_county_wy': {
            'geojson_path': str(project_root / 'geojsons_for_db_upload' / 'sublette_county_wy_data_files' / 'sublette_county_wy_final_ownership.geojson'),
            'county_name': 'Sublette County, WY'
        },
        'teton_county_id': {
            'geojson_path': str(project_root / 'geojsons_for_db_upload' / 'teton_county_id_data_files' / 'teton_county_id_final_ownership.geojson'),
            'county_name': 'Teton County, ID'
        },
        'fremont_county_wy': {
            'geojson_path': str(project_root / 'geojsons_for_db_upload' / 'fremont_county_wy_data_files' / 'fremont_county_wy_final_ownership.geojson'),
            'county_name': 'Fremont County, WY'
        },
        'lincoln_county_wy': {
            'geojson_path': str(project_root / 'geojsons_for_db_upload' / 'lincoln_county_wy_data_files' / 'lincoln_county_wy_final_ownership.geojson'),
            'county_name': 'Lincoln County, WY'
        }
    }

def verify_data_files(counties_data):
    """Verify that all data files exist before processing"""
    missing_files = []
    for county in counties_data:
        geojson_path = county['geojson_path']
        if not os.path.exists(geojson_path):
            missing_files.append(f"{county['county_name']}: {geojson_path}")
    
    if missing_files:
        print("❌ The following data files are missing:")
        for missing in missing_files:
            print(f"   - {missing}")
        print("\nPlease check that the files exist and the paths are correct.")
        return False
    
    print("✅ All data files found!")
    return True

def migrate_to_postgis(counties_to_migrate=None, force_recreate=False):
    """Migrate from current pipeline to PostGIS + Tegola setup."""
    
    # Database configuration - update these for your Google Cloud VM
    db_config = {
        'host': 'localhost',  # Change to your VM's IP when deployed
        'port': 5432,
        'database': 'parcel_data',
        'user': 'noah',
        'password': 'fatcat202'  # Change this!
    }
    
    # Initialize the new pipeline
    pipeline = PostGISPipeline(db_config)
    
    print("=== Migrating geojsons to PostGIS Database===")
    
    # Get all available counties
    all_counties = get_county_data_files()
    
    # Determine which counties to migrate
    if counties_to_migrate is None:
        # Migrate all counties
        counties_data = list(all_counties.values())
        print("Migrating ALL counties to PostGIS...")
    else:
        # Migrate specific counties
        counties_data = []
        for county_key in counties_to_migrate:
            if county_key in all_counties:
                counties_data.append(all_counties[county_key])
                print(f"✅ Will migrate: {all_counties[county_key]['county_name']}")
            else:
                print(f"❌ County not found: {county_key}")
                print(f"   Available counties: {list(all_counties.keys())}")
                return
    
    if not counties_data:
        print("❌ No valid counties to migrate!")
        return
    
    print(f"📊 Total counties to migrate: {len(counties_data)}")
    print()
    
    # Verify all data files exist
    print("Verifying data files...")
    if not verify_data_files(counties_data):
        return
    
    print()
    
    # Step 1: Setup database
    print("Step 1: Setting up PostGIS database...")
    pipeline.setup_database()
    print()
    
    # Force recreate the parcels table to handle MultiPolygons
    print("Recreating parcels table to support MultiPolygons...")
    pipeline.db_manager.create_parcel_table(force_recreate=force_recreate)
    print()
    
    # Step 2: Import county data
    print("Step 2: Importing county data...")
    pipeline.import_all_counties(counties_data)
    print()
    
    
    # Step 4: Show database statistics
    print("Step 4: Database statistics:")
    pipeline.print_database_stats()
    
    # Step 5: Instructions for next steps
    print("=== Migration Complete! ===")
    print()
    print("Next steps:")
    print("1. Install Tegola on your system:")
    print("   - macOS: brew install tegola")
    print("   - Linux: Download from https://github.com/go-spatial/tegola/releases")
    print()
    print("2. Start the Tegola server:")
    print("   tegola serve --config tegola_config.toml")
    print()
    print("3. Test your vector tiles:")
    print("   - Capabilities: http://localhost:8080/maps/parcel_map/capabilities.json")
    print("   - Sample tile: http://localhost:8080/maps/parcel_map/10/195/196.pbf")
    print()
    print("4. Update your web application to use the new tile URLs:")
    print("   Base URL: http://localhost:8080/maps/parcel_map/{z}/{x}/{y}.pbf")
    print()
    print("5. For Google Cloud VM deployment:")
    print("   - Update db_config host to your VM's IP")
    print("   - Install PostgreSQL, PostGIS, and Tegola on the VM")
    print("   - Configure firewall to allow connections on port 8080")
    print("   - Use systemd or similar to run Tegola as a service")

def update_existing_pipeline():
    """Update your existing pipeline to use PostGIS instead of tile generation."""
    
    print("=== Updating Existing Pipeline ===")
    print("This will modify your existing pipeline to use PostGIS instead of tile generation.")
    print()
    
    # Read your existing pipeline configuration
    try:
        with open('download_and_file_config.json', 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print("Error: download_and_file_config.json not found!")
        return
    
    # Create a new configuration that includes PostGIS settings
    postgis_config = {
        'database': {
            'host': 'localhost',
            'port': 5432,
            'database': 'parcel_data',
            'user': 'postgres',
            'password': 'your_secure_password'
        },
        'tegola': {
            'port': 8080,
            'config_file': 'tegola_config.toml'
        },
        'counties': config.get('counties', [])
    }
    
    # Save the new configuration
    with open('postgis_config.json', 'w') as f:
        json.dump(postgis_config, f, indent=2)
    
    print("Created postgis_config.json with your settings.")
    print("Update the database password and host for your Google Cloud VM.")

def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(
        description='Migrate county data to PostGIS database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Migrate all counties
  python migrate_to_postgis.py

  # Migrate specific counties
  python migrate_to_postgis.py --counties teton_county_wy lincoln_county_wy

  # Migrate single county
  python migrate_to_postgis.py --counties teton_county_wy

  # List available counties
  python migrate_to_postgis.py --list-counties

  # Update existing pipeline config
  python migrate_to_postgis.py update
        """
    )
    
    parser.add_argument(
        '--counties', 
        nargs='+', 
        help='Specific counties to migrate (space-separated)'
    )
    
    parser.add_argument(
        '--list-counties', 
        action='store_true',
        help='List all available counties'
    )
    
    parser.add_argument(
        'update', 
        nargs='?', 
        help='Update existing pipeline configuration'
    )
    
    args = parser.parse_args()
    
    # Handle list-counties option
    if args.list_counties:
        all_counties = get_county_data_files()
        print("Available counties:")
        for key, county in all_counties.items():
            print(f"  {key}: {county['county_name']}")
        return
    
    # Handle update option
    if args.update == 'update':
        update_existing_pipeline()
        return
    
    # Run migration
    migrate_to_postgis(args.counties)

if __name__ == "__main__":
    main() 