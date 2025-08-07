import json
import os
import shutil
import sys
from pathlib import Path

# Add the tile_cycle directory to Python path for imports
tile_cycle_dir = Path(__file__).parent
sys.path.insert(0, str(tile_cycle_dir))

from downloading_and_geojson_processing.data_merger import DataMerger
from downloading_and_geojson_processing.data_standardizer import DataStandardizer
from downloading_and_geojson_processing.cloud_gcs_uploader import upload_geojson_to_gcs
from geojson_to_database_cycle.migrate_to_postgis import migrate_to_postgis

# Utility function to clear a directory
def clear_directory(directory):
    if os.path.exists(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')

class CountyFactory:
    """Factory to create the right county class"""
    
    @staticmethod
    def create_county(county_name, output_dir="geojson_files"):
        # Import here to avoid circular imports
        from counties.counties import TetonCountyWy, LincolnCountyWy, SubletteCountyWy, TetonCountyId, FremontCountyWy
        
        county_classes = {
            "teton_county_wy": TetonCountyWy,
            "lincoln_county_wy": LincolnCountyWy,
            "sublette_county_wy": SubletteCountyWy,
            "teton_county_id": TetonCountyId,
            "fremont_county_wy": FremontCountyWy,
        }
        
        if county_name not in county_classes:
            raise ValueError(f"Unknown county: {county_name}")
            
        # Ensure output directory is relative to tile_cycle
        tile_cycle_dir = Path(__file__).parent
        full_output_dir = tile_cycle_dir / f"{county_name}_data_files"
        return county_classes[county_name](county_name, str(full_output_dir))

class OwnershipPipeline:
    """Orchestrates the entire ownership data pipeline"""
    
    def __init__(self, output_dir="Processed_Geojsons"):
        # Ensure paths are relative to tile_cycle directory
        self.tile_cycle_dir = Path(__file__).parent
        self.output_dir = self.tile_cycle_dir / output_dir
        
        # Create DataMerger and DataStandardizer with correct paths
        self.merger = DataMerger(str(self.output_dir))
        config_path = self.tile_cycle_dir / "download_and_file_config.json"
        self.standardizer = DataStandardizer(str(self.output_dir), str(config_path))
        os.makedirs(self.output_dir, exist_ok=True)
    
    def process_county(self, county_name, upload_to_gcs=True, skip_gcs_upload=False):
        """Process a single county through the full pipeline, including tile generation and upload"""
        print(f"🏁 Starting pipeline for {county_name}")
        # Clear relevant directories before processing - all relative to tile_cycle
        county_data_dir = self.tile_cycle_dir / f"{county_name}_data_files"
        db_upload_dir = self.tile_cycle_dir / "geojsons_for_db_upload" / f"{county_name}_data_files"
        clear_directory(str(county_data_dir))
        clear_directory(str(db_upload_dir))
        # Create county instance
        county = CountyFactory.create_county(county_name, self.output_dir)
        # Run county-specific processing
        standardized_data = county.collect_and_organize_county_ownership_data()
        # Upload to GCS
        if upload_to_gcs and not skip_gcs_upload:
            local_geojson_path = self.tile_cycle_dir / "geojsons_for_db_upload" / f"{county_name}_data_files" / f"{county_name}_final_ownership.geojson"
            if local_geojson_path.exists():
                upload_geojson_to_gcs(str(local_geojson_path), county_name)
            else:
                print(f"❌ GeoJSON file not found for upload: {local_geojson_path}")
        elif skip_gcs_upload:
            print(f"⏭️ Skipping GCS upload for {county_name}")
        return standardized_data
    
    def upload_only(self, county_list):
        """Upload finalized geojsons for each county to GCS without processing."""
        for county_name in county_list:
            local_geojson_path = self.tile_cycle_dir / "geojsons_for_db_upload" / f"{county_name}_data_files" / f"{county_name}_final_ownership.geojson"
            if local_geojson_path.exists():
                print(f"Uploading {local_geojson_path} to GCS...")
                upload_geojson_to_gcs(str(local_geojson_path), county_name)
            else:
                print(f"❌ GeoJSON file not found for upload: {local_geojson_path}")

    def process_all_counties(self, county_list, upload_to_gcs=True, skip_gcs_upload=False):
        """Process multiple counties, one at a time, without combining results."""
        print(f"🏁 Starting pipeline for {len(county_list)} counties: {', '.join(county_list)}")

        for county_name in county_list:
            print(f"\n{'='*50}")
            print(f"Processing {county_name}...")
            print(f"{'='*50}")
            try:
                self.process_county(county_name, upload_to_gcs=upload_to_gcs, skip_gcs_upload=skip_gcs_upload)
                print(f"✅ Successfully processed {county_name}")
            except Exception as e:
                print(f"❌ Failed to process {county_name}: {e}")
                continue

        print(f"\n🎉 Pipeline completed for all counties!")
    
    def get_available_counties(self):
        """Get list of available counties"""
        return ["teton_county_wy", "lincoln_county_wy", "sublette_county_wy", "teton_county_id", "fremont_county_wy"]
    
    def validate_county(self, county_name):
        """Validate that a county is supported"""
        available_counties = self.get_available_counties()
        if county_name not in available_counties:
            raise ValueError(f"County '{county_name}' not supported. Available counties: {', '.join(available_counties)}")
        return True

def main():
    """Main function to run the pipeline"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Process county ownership data")
    parser.add_argument("--county", type=str, help="Single county to process")
    parser.add_argument("--all", action="store_true", help="Process all available counties")
    parser.add_argument("--output-dir", type=str, default="Processed_Geojsons", help="Output directory")
    parser.add_argument("--upload-only", action="store_true", help="Skip processing and only upload finalized geojsons to GCS")
    parser.add_argument("--migrate-to-postgis", action="store_true", help="After processing/uploading, migrate all counties to PostGIS")
    parser.add_argument("--migrate-only", action="store_true", help="Skip processing/uploading and only migrate existing geojsons to PostGIS")
    parser.add_argument("--recreate-table", action="store_true", help="Force recreate the parcels table in PostGIS (DROPS ALL DATA)")
    parser.add_argument("--skip-gcs-upload", action="store_true", help="Skip uploading to GCS bucket")
    
    args = parser.parse_args()
    
    pipeline = OwnershipPipeline(
        output_dir=args.output_dir,
    )

    # Determine which counties to operate on
    if args.all:
        county_list = pipeline.get_available_counties()
    elif args.county:
        pipeline.validate_county(args.county)
        county_list = [args.county]
    else:
        print("Please specify either --county <county_name> or --all")
        print(f"Available counties: {', '.join(pipeline.get_available_counties())}")
        print("\nOptional arguments for cloud upload:")
        print("  --bucket-name <name>        GCS bucket name for tile uploads")
        print("  --destination-folder <path> GCS destination folder for tile uploads")
        print("  --tile-output-dir <path>    Local tile output directory")
        return

    # Migrate only mode: skip processing/uploading, just migrate geojsons to PostGIS
    if args.migrate_only:
        print("Migrating existing geojsons to PostGIS...")
        migrate_to_postgis(county_list, force_recreate=args.recreate_table)
        return

    # Upload only mode: skip processing, just upload geojsons to GCS
    if args.upload_only:
        if not args.skip_gcs_upload:
            pipeline.upload_only(county_list)
        else:
            print("⏭️ Skipping GCS upload due to --skip-gcs-upload flag")
        if args.migrate_to_postgis:
            print("Migrating uploaded geojsons to PostGIS...")
            migrate_to_postgis(county_list, force_recreate=args.recreate_table)
        return

    # Normal processing mode
    pipeline.process_all_counties(county_list, upload_to_gcs=True, skip_gcs_upload=args.skip_gcs_upload)
    if args.migrate_to_postgis:
        print("Migrating processed geojsons to PostGIS...")
        migrate_to_postgis(county_list, force_recreate=args.recreate_table)

if __name__ == "__main__":
    main()
