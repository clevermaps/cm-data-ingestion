import requests
import geopandas as gpd
import os

from .settings import GEOBOUNDARIES_URL


def get_data(country_iso, admin_level):

    """
    Fetches geoBoundaries metadata and downloads the corresponding GeoJSON file.
    """
    
    api_url = f"https://www.geoboundaries.org/api/current/gbOpen/{country_iso}/{admin_level}/"
    response = requests.get(api_url)
    response.raise_for_status()
    metadata = response.json()

    gj_url = metadata.get("gjDownloadURL")
    if not gj_url:
        raise ValueError(f"No 'gjDownloadURL' found for {country_iso}-{admin_level}")

    gdf = gpd.read_file(gj_url, engine="pyogrio")

    for _, row in gdf.iterrows():
        
        row_dict = row.to_dict()
        row_dict["geometry"] = row.geometry.wkt
        
        yield row_dict