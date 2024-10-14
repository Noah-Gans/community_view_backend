import os
import requests
import subprocess
from osgeo import ogr
from git import Repo
from datetime import datetime

# URLs for KMZ files
kmz_urls = [
    "https://s3.us-west-2.amazonaws.com/tetoncountywy/gis/download/kmz/conservation_easements.kmz",
    "https://s3.us-west-2.amazonaws.com/tetoncountywy/gis/download/kmz/ownership.kmz",
    "https://s3.us-west-2.amazonaws.com/tetoncountywy/gis/download/kmz/control_points.kmz",
    "https://s3.us-west-2.amazonaws.com/tetoncountywy/gis/download/kmz/plss.kmz",
    "https://s3.us-west-2.amazonaws.com/tetoncountywy/gis/download/kmz/precincts.kmz",
    "https://s3.us-west-2.amazonaws.com/tetoncountywy/gis/download/kmz/roads.kmz",
    "https://s3.us-west-2.amazonaws.com/tetoncountywy/gis/download/kmz/zoning.kmz"
]

# Directory to save KMZ, GeoJSON, and tiles
output_directory = "tile_dir"
os.makedirs(output_directory, exist_ok=True)

def download_file(url, output_path):
    """Download a file from a URL."""
    response = requests.get(url)
    with open(output_path, 'wb') as file:
        file.write(response.content)

def convert_kmz_to_geojson(kmz_file_path, geojson_file_path_base):
    driver = ogr.GetDriverByName('LIBKML')
    datasource = driver.Open(kmz_file_path, 0)  # Open the KMZ file
    if datasource is None:
        print(f"Failed to open KMZ file: {kmz_file_path}")
        return

    for i in range(datasource.GetLayerCount()):
        layer = datasource.GetLayerByIndex(i)
        layer_name = layer.GetName()

        # Ensure that we don't duplicate the base file name in the output file name
        if layer_name.lower() in geojson_file_path_base.lower():
            geojson_file_path = f"{geojson_file_path_base}.geojson"  # Use the base name if it's the same
        else:
            geojson_file_path = f"{geojson_file_path_base}_{layer_name}.geojson"  # Append layer name only if different
        
        geojson_driver = ogr.GetDriverByName('GeoJSON')

        if os.path.exists(geojson_file_path):
            geojson_driver.DeleteDataSource(geojson_file_path)
        
        geojson_datasource = geojson_driver.CreateDataSource(geojson_file_path)
        geojson_layer = geojson_datasource.CreateLayer(layer_name, layer.GetSpatialRef(), layer.GetGeomType())
        
        # Copy fields (attributes)
        geojson_layer.CreateFields(layer.schema)
        
        # Copy features
        for feature in layer:
            geojson_layer.CreateFeature(feature.Clone())
        
        geojson_datasource = None  # Close the GeoJSON file
        
        print(f"Saved {geojson_file_path} to files")

    datasource = None  # Close the KMZ file
    
    # Remove the KMZ file after conversion
    if os.path.exists(kmz_file_path):
        os.remove(kmz_file_path)
        print(f"Deleted {kmz_file_path} after conversion")

def convert_geojson_to_vector_tiles(geojson_file_path, output_tile_dir, min_zoom=6, max_zoom=14):
    """Convert GeoJSON to vector tiles using Tippecanoe."""
    os.makedirs(output_tile_dir, exist_ok=True)
    tile_output = os.path.join(output_tile_dir, 'tiles.mbtiles')

    subprocess.run([
        'tippecanoe',
        '-o', tile_output,
        '--maximum-zoom={}'.format(max_zoom),
        '--minimum-zoom={}'.format(min_zoom),
        '--force',
        geojson_file_path
    ], check=True)

    # Extract the tiles from the mbtiles file
    subprocess.run([
        'tile-join',
        '-e', output_tile_dir,
        '--force',  # Add this option to force overwriting existing files
        tile_output
    ], check=True)

    print(f"Vector tiles saved in {output_tile_dir}")

def push_to_github(repo_path, commit_message):
    """Push the changes to GitHub."""
    repo = Repo(repo_path)

    # Stage, commit, and push changes
    repo.git.add(all=True)  # Stage all changes
    if repo.is_dirty(untracked_files=True):  # Only commit if there are changes
        repo.index.commit(commit_message)  # Commit changes
        origin = repo.remote(name='origin')
        origin.push()  # Push changes to GitHub
        print(f"Changes have been pushed to GitHub with message: {commit_message}.")
    else:
        print("No changes to commit.")

def update_time_file():
    """Update the 'update_time.txt' with the current timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    update_time_path = os.path.join(output_directory, 'update_time.txt')
    
    with open(update_time_path, 'w') as file:
        file.write(f"Last updated on: {timestamp}\n")

    print(f"Updated 'update_time.txt' with the timestamp: {timestamp}")

# Download and convert each file, and push after each file is processed
def driver():
    repo_path = os.getcwd()  # Assuming the script is running in the repo's root directory
    for kmz_url in kmz_urls:
        file_name = os.path.basename(kmz_url)
        kmz_file_path = os.path.join(output_directory, file_name)

        # Download the KMZ file
        print(f"Downloading {file_name}...")
        download_file(kmz_url, kmz_file_path)

        # Convert to GeoJSON
        geojson_file_name = file_name.replace('.kmz', '')  # Remove .kmz extension from file_name
        geojson_file_path_base = os.path.join(output_directory, geojson_file_name)
        print(f"Converting {file_name} to {geojson_file_name}.geojson...")

        # Convert KMZ to GeoJSON (multiple layers possible)
        convert_kmz_to_geojson(kmz_file_path, geojson_file_path_base)

        # Check for all layers that were saved
        for file in os.listdir(output_directory):
            if file.startswith(geojson_file_name) and file.endswith('.geojson'):
                geojson_file_path = os.path.join(output_directory, file)

                # Convert each GeoJSON layer to Vector Tiles
                layer_name = file.replace('.geojson', '')
                tile_output_directory = os.path.join(output_directory, f"{layer_name}_tiles")
                print(f"Converting {file} to vector tiles...")
                convert_geojson_to_vector_tiles(geojson_file_path, tile_output_directory)

                print(f"Saved vector tiles for {file} in {tile_output_directory}\n")

    # Update the time file after all files are processed
    update_time_file()

    # Push to GitHub
    commit_message = "Update GeoJSON files and vector tiles, and update timestamp"
    push_to_github(repo_path, commit_message)


driver()

print("All files have been downloaded, converted to GeoJSON, tiled, and pushed to GitHub.")
