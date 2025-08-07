import json
import os
import logging
from datetime import datetime
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_state_from_county(county_name):
    """Parse state abbreviation from county name"""
    if "_wy" in county_name.lower():
        return "WY"
    elif "_id" in county_name.lower():
        return "ID"
    elif "_mt" in county_name.lower():
        return "MT"
    elif "_ut" in county_name.lower():
        return "UT"
    elif "_co" in county_name.lower():
        return "CO"
    # Add more states as needed
    return ""

def clean_county_name(county_name):
    """Extract clean county name from full county identifier"""
    # Remove state suffix and convert to title case
    # fremont_county_wy -> Fremont County
    # teton_county_id -> Teton County
    # lincoln_county_wy -> Lincoln County
    
    # Split on underscores and take the first part
    parts = county_name.split('_')
    if len(parts) >= 2 and parts[1] == 'county':
        # Take the first part (county name) and capitalize it
        county_base = parts[0].replace('_', ' ').title()
        return f"{county_base} County"
    else:
        # Fallback: just clean up the name
        return county_name.replace('_', ' ').title()

def calculate_bbox(geometry):
    """Calculate bounding box for a geometry"""
    if not geometry or geometry.get('type') not in ['Polygon', 'MultiPolygon']:
        return None
    
    coordinates = geometry.get('coordinates', [])
    if not coordinates:
        return None
    
    # Flatten coordinates to get all points
    def flatten_coords(coords):
        points = []
        if not coords or not isinstance(coords, (list, tuple)) or len(coords) == 0:
            return points
            
        # Handle empty first element
        if len(coords[0]) == 0:
            return points
            
        if isinstance(coords[0], (list, tuple)):
            if isinstance(coords[0][0], (list, tuple)):
                # MultiPolygon or Polygon with holes
                for part in coords:
                    if not part or len(part) == 0:  # Skip empty parts
                        continue
                    if isinstance(part[0][0], (list, tuple)):
                        # MultiPolygon
                        for ring in part:
                            if ring:  # Only process non-empty rings
                                points.extend(ring)
                    else:
                        # Polygon ring
                        points.extend(part)
            else:
                # Simple coordinate array
                points = coords
        return points
    
    all_points = flatten_coords(coordinates)
    if not all_points:
        return None
    
    # Filter and extract coordinates with better error handling
    lons = []
    lats = []
    for point in all_points:
        if isinstance(point, (list, tuple)) and len(point) >= 2:
            try:
                lon = float(point[0])
                lat = float(point[1])
                lons.append(lon)
                lats.append(lat)
            except (ValueError, TypeError, IndexError) as e:
                # Skip invalid coordinate points
                continue
    
    if not lons or not lats:
        return None
    
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)
    
    return [min_lon, min_lat, max_lon, max_lat]

def normalize_text(text):
    """
    Normalize text for consistent searching
    """
    if not text:
        return ""
    return text.lower().strip()

def create_search_index():
    """
    Create a lightweight search index with only essential search fields
    """
    # Define the counties and their data paths  
    counties = [
        "fremont_county_wy",
        "teton_county_id", 
        "sublette_county_wy",
        "lincoln_county_wy",
        "teton_county_wy"
    ]
    
    # Base path for GeoJSON files - navigate to tile_processing from search_api
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    base_path = project_root / "tile_processing" / "geojsons_for_db_upload"
    
    # Initialize the search index
    search_index = []
    
    logger.info("🔍 Creating lightweight search index...")
    
    for county in counties:
        county_path = base_path / f"{county}_data_files" / f"{county}_final_ownership.geojson"
        
        if not county_path.exists():
            logger.warning(f"⚠️  Warning: {county_path} not found, skipping...")
            continue
            
        logger.info(f"📂 Processing {county}...")
        
        try:
            with open(county_path, 'r') as f:
                county_data = json.load(f)
            
            # Parse state from county name
            state = parse_state_from_county(county)
            
            # Process each feature in the county
            feature_count = 0
            for feature in county_data.get('features', []):
                try:
                    feature_count += 1
                    properties = feature.get('properties', {})
                    geometry = feature.get('geometry', {})
                    
                    # Calculate bbox for spatial queries
                    bbox = calculate_bbox(geometry)
                    
                    # Create enhanced search entry with new fields
                    search_entry = {
                        "global_parcel_uid": properties.get("global_parcel_uid", ""),
                        "pidn": properties.get("county_parcel_id_num", ""),
                        "owner": properties.get("owner_name", ""),
                        "mailing_address": properties.get("mailing_address", ""),
                        "physical_address": properties.get("physical_address", ""),
                        "county": clean_county_name(county),
                        "state": state,
                        "bbox": bbox,
                        "clerk_rec": properties.get("clerk_records_link", ""),
                        "property_det": properties.get("property_details_link", ""),
                        "tax_info": properties.get("tax_details_link", "")
                    }
                    
                    search_index.append(search_entry)
                    
                except Exception as feature_error:
                    logger.error(f"❌ Error processing feature {feature_count} in {county}: {feature_error}")
                    continue
                
        except Exception as e:
            logger.error(f"❌ Error processing {county}: {e}")
            import traceback
            logger.error(f"❌ Full traceback: {traceback.format_exc()}")
            continue
    
    # Save the enhanced search index
    output_path = "search_api/search_index.json"
    with open(output_path, 'w') as f:
        json.dump(search_index, f, indent=2)
    
    logger.info(f"✅ Enhanced search index created: {output_path}")
    logger.info(f"📊 Total entries: {len(search_index)}")
    
    # Get file size
    file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
    logger.info(f"📏 File size: {file_size:.2f} MB")
    
    return output_path

def main():
    """
    Main function to create the search index
    """
    logger.info("🚀 Starting search index creation process...")
    
    # Create the search index
    search_index_path = create_search_index()
    
    if search_index_path and os.path.exists(search_index_path):
        logger.info("🎉 Successfully created search index!")
    else:
        logger.error("❌ Failed to create search index")

if __name__ == "__main__":
    main() 