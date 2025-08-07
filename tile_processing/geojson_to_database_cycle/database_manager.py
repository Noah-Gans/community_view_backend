from itertools import count
import psycopg2
import geopandas as gpd
import os
from psycopg2.extras import RealDictCursor
import shapely
import sys
import json
from shapely.wkb import dumps as wkb_dumps

class DatabaseManager:
    def __init__(self, host='localhost', port=5432, database='parcel_data', 
                 user='postgres', password='your_password'):
        self.connection_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
    
    def _repair_geometry(self, geometry):
        """Attempt to repair invalid geometries."""
        from shapely.validation import make_valid
        from shapely.geometry import Polygon, MultiPolygon
        
        try:
            # Try to make the geometry valid
            repaired = make_valid(geometry)
            
            # If it's still invalid, try more aggressive repair
            if not repaired.is_valid:
                # For polygons, try to fix by removing degenerate parts
                if repaired.geom_type == 'Polygon':
                    if repaired.area < 1e-10:  # Very small area
                        return None
                    # Try to simplify and buffer
                    simplified = repaired.simplify(0.000001)
                    buffered = simplified.buffer(0)
                    if buffered.is_valid and buffered.area > 0:
                        return buffered
                elif repaired.geom_type == 'MultiPolygon':
                    # Keep only valid parts
                    valid_parts = []
                    if hasattr(repaired, 'geoms'):
                        for part in repaired.geoms:
                            if part.is_valid and part.area > 1e-10:
                                valid_parts.append(part)
                        if valid_parts:
                            return MultiPolygon(valid_parts)
                return None
            
            return repaired
        except Exception as e:
            print(f"    DEBUG _repair_geometry: Repair failed: {e}")
            return None

    def _validate_and_clean_coordinates(self, geometry):
        """Validate and clean coordinates to prevent PostGIS transformation errors."""
        from shapely.geometry import Polygon, MultiPolygon
        import math

        def is_valid_coord(coord):
            if len(coord) < 2:
                return False
            lon, lat = coord[0], coord[1]
            if math.isnan(lon) or math.isnan(lat) or math.isinf(lon) or math.isinf(lat):
                return False
            if lon < -180 or lon > 180 or lat < -90 or lat > 90:
                return False
            return True

        def clean_coords(coords, label=""):
            original_len = len(coords)
            cleaned = []
            dropped = 0
            for coord in coords:
                if is_valid_coord(coord):
                    cleaned.append((coord[0], coord[1]) if len(coord) > 2 else coord)
                else:
                    dropped += 1
            if dropped > 0:
                print(f"    ⚠️ Dropped {dropped}/{original_len} coords from {label}")
            return cleaned

        try:
            if geometry.geom_type == 'Polygon':
                exterior = clean_coords(list(geometry.exterior.coords), label="exterior ring")
                if len(exterior) < 4:
                    print("    ❌ Exterior ring has too few valid points after cleaning:", len(exterior))
                    return None

                interiors = []
                for i, ring in enumerate(geometry.interiors):
                    interior = clean_coords(list(ring.coords), label=f"interior ring {i}")
                    if len(interior) >= 4:
                        interiors.append(interior)
                    else:
                        print(f"    ❌ Dropped interior ring {i} due to too few points ({len(interior)})")

                poly = Polygon(exterior, interiors)
                if not poly.is_valid:
                    print("    ❌ Polygon is invalid after cleaning")
                    return None
                return poly

            elif geometry.geom_type == 'MultiPolygon':
                valid_parts = []
                for i, part in enumerate(geometry.geoms):
                    cleaned_part = self._validate_and_clean_coordinates(part)
                    if cleaned_part is not None and cleaned_part.is_valid:
                        valid_parts.append(cleaned_part)
                    else:
                        print(f"    ⚠️ Dropped polygon part {i} of MultiPolygon during cleaning")

                if valid_parts:
                    return MultiPolygon(valid_parts)
                else:
                    print("    ⚠️ Entire MultiPolygon dropped: no valid parts remained")
                    return None

            else:
                return geometry

        except Exception as e:
            print(f"    ⚠️ Error cleaning coordinates: {e}")
            return None


    def count_coords(self, geometry):
        if geometry.geom_type == 'Polygon':
            return len(list(geometry.exterior.coords))
        elif geometry.geom_type == 'MultiPolygon':
            return sum(len(list(p.exterior.coords)) for p in geometry.geoms)
        else:
            return 0

    def safe_geometry_to_wkb(self, geometry, parcel_id=None, max_points=1000):
        """Safely convert geometry to WKB, handling 3D to 2D conversion."""
        # Special debugging for the problematic parcel
        if parcel_id == 'teton_county_wy_000001':
            print(f"    🔍 safe_geometry_to_wkb for {parcel_id} (22-38-15-01-1-00-001):")
            print(f"      Input geometry type: {geometry.geom_type}")
            print(f"      Input geometry area: {geometry.area}")
            print(f"      Input geometry bounds: {geometry.bounds}")
            print(f"      Original coordinate count: {self.count_coords(geometry)}")
            if hasattr(geometry, 'geoms'):
                print(f"      Input MultiPolygon parts: {len(list(geometry.geoms))}")
        
        # Check for null geometry first
        if geometry is None:
            print("    DEBUG safe_geometry_to_wkb: Geometry is None")
            return None
            
        try:
            # Check if geometry is empty
            if geometry.is_empty:
                print("    DEBUG safe_geometry_to_wkb: Geometry is empty")
                return None
                
             # Check if geometry is valid first
            if not geometry.is_valid:
                print(f"    DEBUG: parcel_id={parcel_id}")
                print(f"    DEBUG safe_geometry_to_wkb: Geometry is invalid")
                print(f"    DEBUG safe_geometry_to_wkb: Geometry type: {geometry.geom_type}")
                print(f"    DEBUG safe_geometry_to_wkb: Geometry area: {geometry.area}")
                print(f"    DEBUG safe_geometry_to_wkb: Geometry length: {geometry.length}")
                print(f"    DEBUG safe_geometry_to_wkb: Geometry bounds: {geometry.bounds}")
                
                # Try to repair the geometry
                repaired_geometry = self._repair_geometry(geometry)
                if repaired_geometry is not None and repaired_geometry.is_valid:
                    print(f"    DEBUG safe_geometry_to_wkb: Geometry repaired successfully")
                    geometry = repaired_geometry
                else:
                    print(f"    DEBUG safe_geometry_to_wkb: Could not repair geometry")
                    return None
                
            # Validate and clean coordinates first
            cleaned_geometry = self._validate_and_clean_coordinates(geometry)
            
            if cleaned_geometry is None:
                print("    DEBUG safe_geometry_to_wkb: Coordinate validation failed")
                return None
                
            
                
            geometry = cleaned_geometry
            if parcel_id == '22-38-15-01-1-00-001':
                print(f"    🔍 safe_geometry_to_wkb after cleaning for 22-38-15-01-1-00-001:")
                print(f"      Input geometry type: {geometry.geom_type}")
                print(f"      Input geometry area: {geometry.area}")
                print(f"      Input geometry bounds: {geometry.bounds}")
                print(f"      Original coordinate count: {self.count_coords(geometry)}")
                if hasattr(geometry, 'geoms'):
                    print(f"      Input MultiPolygon parts: {len(list(geometry.geoms))}")
                
           
            # Handle 3D to 2D conversion
            geom_2d = self._convert_3d_to_2d(geometry)
            if geom_2d is None:
                print("    DEBUG safe_geometry_to_wkb: 3D to 2D conversion returned None")
                return None
                
            # Special debugging for the problematic parcel
            if parcel_id == '22-38-15-01-1-00-001':
                print(f"    🔍 safe_geometry_to_wkb after 2d-3d for 22-38-15-01-1-00-001:")
                print(f"      Input geometry type: {geometry.geom_type}")
                print(f"      Input geometry area: {geometry.area}")
                print(f"      Input geometry bounds: {geometry.bounds}")
                print(f"      Original coordinate count: {self.count_coords(geometry)}")
                if hasattr(geometry, 'geoms'):
                    print(f"      Input MultiPolygon parts: {len(list(geometry.geoms))}")
                
            # Check if the 2D geometry is still valid
            if not geom_2d.is_valid:
                print(f"    DEBUG safe_geometry_to_wkb: 2D geometry is invalid, attempting repair")
                repaired_2d = self._repair_geometry(geom_2d)
                if repaired_2d is not None and repaired_2d.is_valid:
                    geom_2d = repaired_2d
                else:
                    print(f"    DEBUG safe_geometry_to_wkb: Could not repair 2D geometry")
                    return None
                
            # Use the working WKB conversion method we discovered
            try:
                wkb = wkb_dumps(geom_2d)
                #print(f"    DEBUG safe_geometry_to_wkb: WKB conversion successful, length: {len(wkb)}")
                return wkb
            except (RecursionError, MemoryError, ValueError) as e:
                #print(f"    DEBUG safe_geometry_to_wkb: WKB conversion failed: {e}")
                return None
            
        except (RecursionError, MemoryError, ValueError) as e:
            print(f"    DEBUG safe_geometry_to_wkb: Exception in main try block: {e}")
            return None
        except Exception as e:
            print(f"    DEBUG safe_geometry_to_wkb: Unexpected exception: {e}")
            return None
    
    def _convert_3d_to_2d(self, geometry):
        from shapely.geometry import Polygon, MultiPolygon, shape, mapping
        import json

        def is_3d(coords):
            """Check if any coordinate has a Z dimension"""
            for coord in coords:
                if len(coord) > 2:
                    return True
            return False

        def strip_z(coords):
            return [(x, y) for x, y, *_ in coords]

        try:
            if geometry.geom_type == 'Polygon':
                if not is_3d(geometry.exterior.coords):
                    #print(f"    DEBUG _convert_3d_to_2d: Polygon already 2D")
                    return geometry  # Already 2D

                exterior = strip_z(geometry.exterior.coords)
                interiors = [strip_z(ring.coords) for ring in geometry.interiors]
                return Polygon(exterior, interiors)

            elif geometry.geom_type == 'MultiPolygon':
                # Only run conversion if any of the polygons are 3D
                if all(not is_3d(p.exterior.coords) for p in geometry.geoms):
                    #print(f"    DEBUG _convert_3d_to_2d: Polygon already 2D")

                    return geometry  # Already 2D

                valid_polys = []
                for p in geometry.geoms:
                    exterior = strip_z(p.exterior.coords)
                    interiors = [strip_z(ring.coords) for ring in p.interiors]
                    poly = Polygon(exterior, interiors)
                    if poly.is_valid and len(exterior) >= 4:
                        valid_polys.append(poly)
                if valid_polys:
                    return MultiPolygon(valid_polys)
                else:
                    print("    ⚠️ No valid polygons after 3D→2D conversion")
                    return None

            else:
                return shape(json.loads(json.dumps(mapping(geometry))))

        except Exception as e:
            print(f"    DEBUG _convert_3d_to_2d: Error in conversion: {e}")
            # Fallback using WKT
            try:
                wkt_2d = geometry.wkt.replace(' Z', '').replace(' z', '')
                from shapely.wkt import loads
                return loads(wkt_2d)
            except Exception as wkt_error:
                print(f"    DEBUG _convert_3d_to_2d: WKT conversion also failed: {wkt_error}")
                return None


    def create_database_and_extensions(self):
        """Create database and enable PostGIS extension."""
        # Connect to default postgres database first
        conn = psycopg2.connect(
            host=self.connection_params['host'],
            port=self.connection_params['port'],
            database='postgres',
            user=self.connection_params['user'],
            password=self.connection_params['password']
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Create database if it doesn't exist
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{self.connection_params['database']}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {self.connection_params['database']}")
            print(f"Created database: {self.connection_params['database']}")
        
        conn.close()
        
        # Connect to the new database and enable PostGIS
        conn = psycopg2.connect(**self.connection_params)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Enable PostGIS extension
        cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis_topology")
        print("PostGIS extensions enabled")
        
        conn.close()
    
    def create_parcel_table(self, table_name='parcels', force_recreate=False):
        """Create the parcels table with spatial indexing."""
        conn = psycopg2.connect(**self.connection_params)
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        if force_recreate and table_exists:
            print(f"Dropping existing table: {table_name}")
            cursor.execute(f"DROP TABLE {table_name} CASCADE;")
            table_exists = False
        
        if not table_exists:
            create_table_sql = f"""
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                global_parcel_uid VARCHAR(255) UNIQUE NOT NULL,
                county_parcel_id_num VARCHAR(255),
                owner_name TEXT,
                physical_address TEXT,
                mailing_address TEXT,
                acreage NUMERIC,
                property_value NUMERIC,
                land_type_description TEXT,
                deed_reference TEXT,
                owner_city TEXT,
                owner_state TEXT,
                owner_zip TEXT,
                property_details_link TEXT,
                tax_details_link TEXT,
                clerk_records_link TEXT,
                address TEXT,
                county VARCHAR(100),
                state VARCHAR(10),
                geometry GEOMETRY,
                has_spatial_data BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor.execute(create_table_sql)
            print(f"Created table: {table_name}")
        else:
            # Add global_parcel_uid column if it doesn't exist
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = '{table_name}' AND column_name = 'global_parcel_uid'
                );
            """)
            global_uid_exists = cursor.fetchone()[0]
            
            if not global_uid_exists:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN global_parcel_uid VARCHAR(255);")
                print(f"Added global_parcel_uid column to existing table: {table_name}")
            
            # Add has_spatial_data column if it doesn't exist
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = '{table_name}' AND column_name = 'has_spatial_data'
                );
            """)
            column_exists = cursor.fetchone()[0]
            
            if not column_exists:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN has_spatial_data BOOLEAN DEFAULT FALSE;")
                print(f"Added has_spatial_data column to existing table: {table_name}")
            else:
                print(f"Table {table_name} already exists with all required columns")
        
        # Create indexes (will be ignored if they already exist)
        indexes_sql = f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_geometry 
        ON {table_name} USING GIST (geometry);
        
        CREATE INDEX IF NOT EXISTS idx_{table_name}_county 
        ON {table_name} (county);
        
        CREATE INDEX IF NOT EXISTS idx_{table_name}_global_parcel_uid 
        ON {table_name} (global_parcel_uid);
        
        CREATE INDEX IF NOT EXISTS idx_{table_name}_parcel_id 
        ON {table_name} (county_parcel_id_num);
        
        CREATE INDEX IF NOT EXISTS idx_{table_name}_has_spatial 
        ON {table_name} (has_spatial_data);
        """
        
        cursor.execute(indexes_sql)
        conn.commit()
        conn.close()
    
    def import_geojson_to_postgis(self, geojson_file_path, table_name='parcels', county_name=None):
        """Import GeoJSON data into PostGIS table."""
        # Read GeoJSON with geopandas
        gdf = gpd.read_file(geojson_file_path)
        
        if gdf is None or len(gdf) == 0:
            print(f"Warning: No data found in {geojson_file_path}")
            return
        
        print(f"DEBUG: Initial features loaded: {len(gdf)}")
        
        # Ensure geometry is in WGS84 (EPSG:4326)
        if gdf.crs != 'EPSG:4326':
            gdf = gdf.to_crs('EPSG:4326')
        
        # Add county column if not present
        if 'county' not in gdf.columns and county_name:
            gdf['county'] = county_name
        
        # Filter out rows with null geometries
        initial_count = len(gdf)
        gdf = gdf.dropna(subset=['geometry'])
        null_geometries_dropped = initial_count - len(gdf)
        print(f"DEBUG: Dropped {null_geometries_dropped} features with null geometries")
        
        # Filter out invalid geometries (only for non-null geometries)
        if len(gdf) > 0:
            # Only check validity for non-null geometries
            valid_mask = gdf.geometry.is_valid
            invalid_count = (~valid_mask).sum()
            gdf = gdf[valid_mask]
            print(f"DEBUG: Dropped {invalid_count} features with invalid geometries")
        
        if len(gdf) == 0:
            print(f"Warning: No valid geometries found in {geojson_file_path}")
            return
        
        print(f"DEBUG: Final features to import: {len(gdf)}")
        
        # Connect to database
        conn = psycopg2.connect(**self.connection_params)
        
        # Import to PostGIS
        gdf.to_postgis(table_name, conn, if_exists='append', index=False)
        
        conn.close()
        print(f"Imported {len(gdf)} valid features from {geojson_file_path} to {table_name}")
    
    def _clean_properties_for_json(self, properties_dict):
        """Clean properties dictionary to remove NaN values that cause JSON parsing errors."""
        import math
        
        def clean_value(value):
            if isinstance(value, float) and math.isnan(value):
                return None
            elif isinstance(value, str) and value.lower() in ['nan', 'null', '']:
                return None
            elif isinstance(value, dict):
                return {k: clean_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [clean_value(v) for v in value]
            else:
                return value
        
        return clean_value(properties_dict)

    def _parse_numeric(self, val):
        try:
            if val in [None, '', 'null', 'nan']:
                return None
            return float(val)
        except Exception:
            return None

    def update_parcel_data(self, geojson_file_path, table_name='ownership', county_name=None):
        """Update existing parcel data or insert new records with proper transaction handling."""
        gdf = gpd.read_file(geojson_file_path)
        print(f"DEBUG: Initial features loaded: {len(gdf)}")

        if gdf.crs != 'EPSG:4326':
            gdf = gdf.to_crs('EPSG:4326')

        if 'county' not in gdf.columns and county_name:
            gdf['county'] = county_name

        conn = psycopg2.connect(**self.connection_params)
        conn.autocommit = False  # Enable proper transaction handling
        cursor = conn.cursor()

        successful_inserts = 0
        skipped_rows = 0
        spatial_count = 0
        non_spatial_count = 0
        geometry_conversion_failures = 0
        null_geometries = 0
        empty_geometries = 0
        processed_parcel_ids = set()  # Track processed parcel IDs
        successful_parcel_ids = set()  # Track successfully inserted parcel IDs
        failed_parcel_ids = set()  # Track failed parcel IDs
        duplicate_parcel_ids = set()  # Track duplicate parcel IDs
        parcel_id_count = {}  # Track count of each parcel ID

        batch_size = 1000
        total_rows = len(gdf)
        print(f"Processing {total_rows} features in batches of {batch_size}...")

        for batch_start in range(0, total_rows, batch_size):
            batch_end = min(batch_start + batch_size, total_rows)
            batch_df = gdf.iloc[batch_start:batch_end]
            print(f"Processing batch {batch_start//batch_size + 1}/{(total_rows + batch_size - 1)//batch_size} (rows {batch_start+1}-{batch_end})")

            for idx, row in batch_df.iterrows():
                try:
                    global_parcel_uid = row.get('global_parcel_uid', '')
                    parcel_id = row.get('county_parcel_id_num', str(idx))
                    owner_name = row.get('owner_name', '')
                    address = row.get('physical_address', '')
                    acreage = self._parse_numeric(row.get('acreage', ''))
                    property_value = self._parse_numeric(row.get('property_value', ''))
                    land_type = row.get('land_type/description', '')
                    deed_reference = row.get('deed_reference', '')
                    owner_city = row.get('owner_city', '')
                    owner_state = row.get('owner_state', '')
                    owner_zip = row.get('owner_zip', '')
                    property_details_link = row.get('property_details_link', '')
                    tax_details_link = row.get('tax_details_link', '')
                    clerk_records_link = row.get('clerk_records_link', '')
                    mailing_address = row.get('mailing_address', '')

                    # Track processed parcel IDs
                    if global_parcel_uid is not None:
                        processed_parcel_ids.add(global_parcel_uid)
                        if global_parcel_uid in parcel_id_count:
                            parcel_id_count[global_parcel_uid] += 1
                            duplicate_parcel_ids.add(global_parcel_uid)
                        else:
                            parcel_id_count[global_parcel_uid] = 1

                    # Only keep the specified properties
                    simple_props = {
                        'global_parcel_uid': global_parcel_uid,
                        'county_parcel_id_num': parcel_id,
                        'owner_name': owner_name,
                        'physical_address': address,
                        'mailing_address': mailing_address,
                        'acreage': acreage,
                        'property_value': property_value,
                        'land_type/description': land_type,
                        'deed_reference': deed_reference,
                        'owner_city': owner_city,
                        'owner_state': owner_state,
                        'owner_zip': owner_zip,
                        'property_details_link': property_details_link,
                        'tax_details_link': tax_details_link,
                        'clerk_records_link': clerk_records_link
                    }

                    # Clean properties to remove NaN values
                    cleaned_props = self._clean_properties_for_json(simple_props)
                    properties_json = json.dumps(cleaned_props)

                    # Check if parcel exists
                    cursor.execute(
                        f"SELECT id FROM {table_name} WHERE global_parcel_uid = %s",
                        (global_parcel_uid,)
                    )

                    has_valid_geometry = False
                    wkb_geometry = None

                    if row.geometry is not None and not row.geometry.is_empty:
                        wkb_geometry = self.safe_geometry_to_wkb(row.geometry, global_parcel_uid)
                        has_valid_geometry = (wkb_geometry is not None)
                        if not has_valid_geometry:
                            geometry_conversion_failures += 1
                    else:
                        if row.geometry is None:
                            null_geometries += 1
                        else:
                            empty_geometries += 1

                    if cursor.fetchone():
                        # Update existing record
                        if has_valid_geometry:
                            try:
                                update_sql = f"""
                                UPDATE {table_name} 
                                SET owner_name = %s, physical_address = %s, mailing_address = %s, acreage = %s, property_value = %s,
                                    land_type_description = %s, deed_reference = %s, owner_city = %s, owner_state = %s, owner_zip = %s,
                                    property_details_link = %s, tax_details_link = %s, clerk_records_link = %s,
                                    address = %s, county = %s, state = %s, geometry = ST_GeomFromWKB(%s, 4326),
                                    has_spatial_data = TRUE, updated_at = CURRENT_TIMESTAMP
                                WHERE global_parcel_uid = %s
                                """
                                cursor.execute(update_sql, (
                                    owner_name, address, mailing_address, acreage, property_value,
                                    land_type, deed_reference, owner_city, owner_state, owner_zip,
                                    property_details_link, tax_details_link, clerk_records_link,
                                    address, row.get('county', county_name), row.get('state', ''), wkb_geometry,
                                    global_parcel_uid
                                ))
                                spatial_count += 1
                                successful_parcel_ids.add(global_parcel_uid)
                            except Exception as geom_error:
                                print(f"Error processing geometry for row {idx}: {geom_error}")
                                geometry_conversion_failures += 1
                                # Fall back to non-spatial update
                                update_sql = f"""
                                UPDATE {table_name} 
                                SET owner_name = %s, physical_address = %s, mailing_address = %s, acreage = %s, property_value = %s,
                                    land_type_description = %s, deed_reference = %s, owner_city = %s, owner_state = %s, owner_zip = %s,
                                    property_details_link = %s, tax_details_link = %s, clerk_records_link = %s,
                                    address = %s, county = %s, state = %s, geometry = NULL,
                                    has_spatial_data = FALSE, updated_at = CURRENT_TIMESTAMP
                                WHERE global_parcel_uid = %s
                                """
                                cursor.execute(update_sql, (
                                    owner_name, address, mailing_address, acreage, property_value,
                                    land_type, deed_reference, owner_city, owner_state, owner_zip,
                                    property_details_link, tax_details_link, clerk_records_link,
                                    address, row.get('county', county_name), row.get('state', ''), global_parcel_uid
                                ))
                                non_spatial_count += 1
                                successful_parcel_ids.add(global_parcel_uid)
                        else:
                            # Update without geometry
                            update_sql = f"""
                            UPDATE {table_name} 
                            SET owner_name = %s, physical_address = %s, mailing_address = %s, acreage = %s, property_value = %s,
                                land_type_description = %s, deed_reference = %s, owner_city = %s, owner_state = %s, owner_zip = %s,
                                property_details_link = %s, tax_details_link = %s, clerk_records_link = %s,
                                address = %s, county = %s, state = %s, geometry = NULL,
                                has_spatial_data = FALSE, updated_at = CURRENT_TIMESTAMP
                            WHERE global_parcel_uid = %s
                            """
                            cursor.execute(update_sql, (
                                owner_name, address, mailing_address, acreage, property_value,
                                land_type, deed_reference, owner_city, owner_state, owner_zip,
                                property_details_link, tax_details_link, clerk_records_link,
                                address, row.get('county', county_name), row.get('state', ''), global_parcel_uid
                            ))
                            non_spatial_count += 1
                            successful_parcel_ids.add(global_parcel_uid)
                    else:
                        # Insert new record
                        if has_valid_geometry:
                            try:
                                insert_sql = f"""
                                INSERT INTO {table_name} (
                                    global_parcel_uid, county_parcel_id_num, owner_name, physical_address, mailing_address, acreage, property_value,
                                    land_type_description, deed_reference, owner_city, owner_state, owner_zip,
                                    property_details_link, tax_details_link, clerk_records_link,
                                    address, county, state, geometry, has_spatial_data
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s,
                                    %s, %s, %s,
                                    %s, %s, %s, ST_GeomFromWKB(%s, 4326), TRUE
                                )
                                """
                                cursor.execute(insert_sql, (
                                    global_parcel_uid, parcel_id, owner_name, address, mailing_address, acreage, property_value,
                                    land_type, deed_reference, owner_city, owner_state, owner_zip,
                                    property_details_link, tax_details_link, clerk_records_link,
                                    address, row.get('county', county_name), row.get('state', ''), wkb_geometry
                                ))
                                spatial_count += 1
                                successful_parcel_ids.add(global_parcel_uid)
                            except Exception as geom_error:
                                print(f"Error processing geometry for row {idx}: {geom_error}")
                                geometry_conversion_failures += 1
                                failed_parcel_ids.add(global_parcel_uid)
                                # Fall back to non-spatial insert
                                insert_sql = f"""
                                INSERT INTO {table_name} (
                                    global_parcel_uid, county_parcel_id_num, owner_name, physical_address, mailing_address, acreage, property_value,
                                    land_type_description, deed_reference, owner_city, owner_state, owner_zip,
                                    property_details_link, tax_details_link, clerk_records_link,
                                    address, county, state, geometry, has_spatial_data
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s,
                                    %s, %s, %s,
                                    %s, %s, %s, NULL, FALSE
                                )
                                """
                                cursor.execute(insert_sql, (
                                    global_parcel_uid, parcel_id, owner_name, address, mailing_address, acreage, property_value,
                                    land_type, deed_reference, owner_city, owner_state, owner_zip,
                                    property_details_link, tax_details_link, clerk_records_link,
                                    address, row.get('county', county_name), row.get('state', '')
                                ))
                                non_spatial_count += 1
                                successful_parcel_ids.add(global_parcel_uid)
                        else:
                            # Insert without geometry
                            insert_sql = f"""
                            INSERT INTO {table_name} (
                                global_parcel_uid, county_parcel_id_num, owner_name, physical_address, mailing_address, acreage, property_value,
                                land_type_description, deed_reference, owner_city, owner_state, owner_zip,
                                property_details_link, tax_details_link, clerk_records_link,
                                address, county, state, geometry, has_spatial_data
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s,
                                %s, %s, %s,
                                %s, %s, %s, NULL, FALSE
                            )
                            """
                            cursor.execute(insert_sql, (
                                global_parcel_uid, parcel_id, owner_name, address, mailing_address, acreage, property_value,
                                land_type, deed_reference, owner_city, owner_state, owner_zip,
                                property_details_link, tax_details_link, clerk_records_link,
                                address, row.get('county', county_name), row.get('state', '')
                            ))
                            non_spatial_count += 1
                            successful_parcel_ids.add(global_parcel_uid)

                    successful_inserts += 1

                except Exception as e:
                    print(f"Error processing row {idx}: {e}")
                    failed_parcel_ids.add(global_parcel_uid)
                    skipped_rows += 1
                    conn.rollback()  # Rollback on error
                    continue

            # Commit after each batch
            try:
                conn.commit()
            except Exception as e:
                print(f"Error committing batch: {e}")
                conn.rollback()

        conn.close()
        print(f"Successfully processed {successful_inserts} features from {geojson_file_path}")
        print(f"  - Spatial features: {spatial_count}")
        print(f"  - Non-spatial features: {non_spatial_count}")
        print(f"  - Geometry conversion failures: {geometry_conversion_failures}")
        print(f"  - Null geometries: {null_geometries}")
        print(f"  - Empty geometries: {empty_geometries}")
        if skipped_rows > 0:
            print(f"  - Skipped {skipped_rows} invalid features")

        print(f"DEBUG: Processed {len(processed_parcel_ids)} unique parcel IDs")
        print(f"DEBUG: First 10 processed parcel IDs: {sorted(list(processed_parcel_ids))[:10]}")
        print(f"\n=== DISCREPANCY ANALYSIS ===")
        print(f"successful_inserts: {successful_inserts}")
        print(f"len(processed_parcel_ids): {len(processed_parcel_ids)}")
        print(f"len(successful_parcel_ids): {len(successful_parcel_ids)}")
        print(f"len(failed_parcel_ids): {len(failed_parcel_ids)}")
        print(f"Difference: {successful_inserts - len(processed_parcel_ids)}")

        if duplicate_parcel_ids:
            print(f"\n🔍 DUPLICATE PARCEL IDs FOUND ({len(duplicate_parcel_ids)}):")
            for pid in sorted(duplicate_parcel_ids):
                count = parcel_id_count[pid]
                print(f"  - {pid} (appears {count} times)")

        if successful_inserts != len(processed_parcel_ids):
            print(f"\n⚠️  DISCREPANCY DETECTED!")
            processed_not_successful = processed_parcel_ids - successful_parcel_ids
            successful_not_processed = successful_parcel_ids - processed_parcel_ids
            print(f"\nParcel IDs processed but NOT successfully inserted ({len(processed_not_successful)}):")
            for pid in sorted(processed_not_successful):
                print(f"  - {pid}")
            print(f"\nParcel IDs successfully inserted but NOT in processed list ({len(successful_not_processed)}):")
            for pid in sorted(successful_not_processed):
                print(f"  - {pid}")
            empty_parcel_ids = [pid for pid in processed_parcel_ids if not pid or pid.strip() == '']
            if empty_parcel_ids:
                print(f"\nEmpty parcel IDs found: {len(empty_parcel_ids)}")
                print(f"Empty parcel IDs: {empty_parcel_ids}")
            from collections import Counter
            pid_counts = Counter()
            print(f"\nPotential causes:")
            print(f"1. Duplicate parcel IDs in source data")
            print(f"2. Empty/null parcel IDs")
            print(f"3. Database constraint violations")
            print(f"4. Processing errors that weren't caught")
        else:
            print(f"✅ No discrepancy - all processed features have unique parcel IDs")
    
    def get_connection(self):
        """Get a database connection."""
        return psycopg2.connect(**self.connection_params) 