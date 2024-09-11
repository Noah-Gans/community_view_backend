import os
import requests
from osgeo import ogr
from git import Repo


# URLs for KMZ files
kmz_urls = [
    "https://s3.us-west-2.amazonaws.com/tetoncountywy/gis/download/kmz/conservation_easements.kmz",
    "https://s3.us-west-2.amazonaws.com/tetoncountywy/gis/download/kmz/control_points.kmz",
    "https://s3.us-west-2.amazonaws.com/tetoncountywy/gis/download/kmz/ownership.kmz",
    "https://s3.us-west-2.amazonaws.com/tetoncountywy/gis/download/kmz/plss.kmz",
    "https://s3.us-west-2.amazonaws.com/tetoncountywy/gis/download/kmz/precincts.kmz",
    "https://s3.us-west-2.amazonaws.com/tetoncountywy/gis/download/kmz/roads.kmz",
    "https://s3.us-west-2.amazonaws.com/tetoncountywy/gis/download/kmz/vegetation.kmz",
    "https://s3.us-west-2.amazonaws.com/tetoncountywy/gis/download/kmz/zoning.kmz"
]

# Directory to save KMZ and GeoJSON files
output_directory = "files"
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
        
        # Create individual GeoJSON files for each layer
        geojson_file_path = f"{geojson_file_path_base}_{layer_name}.geojson"
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

def push_to_github(repo_path, commit_message):
    """Push the changes to GitHub."""
    repo = Repo(repo_path)

    # Stash local changes if any
    if repo.is_dirty(untracked_files=True):
        print("Stashing local changes.")
        repo.git.stash('save')

    # Ensure we're on the main branch
    if repo.head.is_detached:
        print("Detached head detected, checking out the main branch.")
        try:
            repo.git.checkout('main')
        except Exception as e:
            print(f"Error checking out main branch: {e}")
            return

    # Apply the stash back after checkout
    if repo.git.stash('list'):
        print("Applying stashed changes.")
        repo.git.stash('pop')

    # Stage, commit, and push changes
    repo.git.add(all=True)  # Stage all changes
    if repo.is_dirty(untracked_files=True):  # Only commit if there are changes
        repo.index.commit(commit_message)  # Commit changes
        origin = repo.remote(name='origin')
        origin.push()  # Push changes to GitHub
        print("Changes have been pushed to GitHub.")
    else:
        print("No changes to commit.")

# Download and convert each file
def driver():
    for kmz_url in kmz_urls:
        file_name = os.path.basename(kmz_url)
        kmz_file_path = os.path.join(output_directory, file_name)

        # Download the KMZ file
        print(f"Downloading {file_name}...")
        download_file(kmz_url, kmz_file_path)

        # Convert to GeoJSON
        geojson_file_name = file_name.replace('.kmz', '.geojson')
        geojson_file_path = os.path.join(output_directory, geojson_file_name)
        print(f"Converting {file_name} to {geojson_file_name}...")
        convert_kmz_to_geojson(kmz_file_path, geojson_file_path)

        print(f"Saved {geojson_file_name} to {output_directory}\n")

# Push the files to the GitHub repository
driver()
repo_path = os.getcwd()  # Assuming the script is running in the repo's root directory
commit_message = "Add converted GeoJSON files"
push_to_github(repo_path, commit_message)

print("All files have been downloaded, converted, and pushed to GitHub.")
