import os
import requests
import zipfile
import geopandas as gpd
import dlt
from cm_data_ingestion.settings import TEMP_DIR
from cm_data_ingestion.sources.openstreetmap.settings import GEOFABRIK_INDEX_URL


def get_country_by_iso_code(iso_code):
    response = requests.get(GEOFABRIK_INDEX_URL)
    response.raise_for_status()  # Raise an error for bad HTTP responses

    data = response.json()
    for item in data.get("features", []):
        properties = item.get("properties", {})
        country_codes = properties.get("iso3166-1:alpha2", [])
        sanitized_country_codes = [code.lower() for code in country_codes]
        if iso_code.lower() in sanitized_country_codes:
            return properties

    return None  # Return None if no match is found

def download_zip(url, output_path):
    response = requests.get(url)
    with open(output_path, "wb") as file:
        file.write(response.content)

def extract_zip(zip_file_path, extract_to="extracted"):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

def find_shp_file(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".shp") and 'places_free' in file:
                return os.path.join(root, file)
    return None

def load_shp_to_geopandas(shp_file_path):
    gdf = gpd.read_file(shp_file_path)
    return gdf


def get_data(country_code, tag, value, release):
    country_data = get_country_by_iso_code(country_code)

    if not country_data:
        raise ValueError(f"No data found for ISO code: {country_code}")

    url = country_data.get("urls", {}).get("shp")

    if not url:
        raise ValueError(f"No SHP URL found for ISO code: {country_code}")

    # Step 1: Download and extract the ZIP file
    zip_file_path = TEMP_DIR + "/downloaded_data.zip"
    extracted_dir = TEMP_DIR + "/extracted"
    download_zip(url, zip_file_path)
    extract_zip(zip_file_path, extracted_dir)

    # Step 2: Load the SHP file using GeoPandas
    shp_file_path = find_shp_file(extracted_dir)
    if shp_file_path is None:
        raise Exception("No SHP file found in the extracted contents")
    gdf_result = load_shp_to_geopandas(shp_file_path)

    # Step 3:
    total_nodes = len(gdf_result)
    print(f"Total nodes fetched: {total_nodes}")

    for index, row in gdf_result.iterrows():

        if (index + 1) % 100 == 0 or index + 1 == total_nodes:
            print(f"Processed {index + 1}/{total_nodes} nodes")

        yield {
            "id": index,
            "geometry": row['geometry'].wkt,  # Convert geometry to WKT format
            "properties": row.drop("geometry").to_json()  # Convert properties to JSON string
        }
