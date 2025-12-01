import requests
import geopandas as gpd
import pycountry
import logging

from .settings import GEOBOUNDARIES_URL

logger = logging.getLogger(__name__)


def get_data(api_url):
    """
    Fetches geoBoundaries metadata and downloads the corresponding GeoJSON file.

    Args:
        api_url

    Yields:
        dict: GeoJSON features as dictionaries with WKT geometry.
    """

    logger.info(f"Fetching geoBoundaries data for: {api_url}")

    try:
        response = requests.get(api_url)
        response.raise_for_status()
        metadata = response.json()
        logger.debug(f"Received metadata: {metadata}")
    except requests.RequestException as e:
        logger.error(f"Failed to fetch metadata from {api_url}: {e}")
        raise

    gj_url = metadata.get("gjDownloadURL")
    if not gj_url:
        error_msg = f"No 'gjDownloadURL' found for {api_url}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(f"Downloading GeoJSON from {gj_url}")
    try:
        gdf = gpd.read_file(gj_url, engine="pyogrio")
        logger.info(f"Downloaded GeoJSON with {len(gdf)} features")
    except Exception as e:
        logger.error(f"Failed to read GeoJSON from {gj_url}: {e}")
        raise

    for _, row in gdf.iterrows():
        row_dict = row.to_dict()
        row_dict["geometry"] = row.geometry.wkt
        logger.debug(f"Yielding row with keys: {list(row_dict.keys())}")
        yield row_dict
