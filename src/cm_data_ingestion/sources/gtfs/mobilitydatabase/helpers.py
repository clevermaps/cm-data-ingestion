import os
import io
import csv
import zipfile
import requests
import dlt
import logging
from .settings import CATALOG_SCHEDULE_URL

logger = logging.getLogger(__name__)


def get_auth_headers():
    """
    Retrieve GitHub authorization headers if GITHUB_TOKEN is set in environment variables.

    Returns:
        dict: Headers containing the authorization token if available, otherwise empty dict.
    """
    token = os.getenv("GITHUB_TOKEN")
    return {"Authorization": f"token {token}"} if token else {}


def fetch_json(url, headers=None):
    """
    Fetch JSON data from a URL with optional headers.

    Args:
        url (str): The URL to fetch JSON from.
        headers (dict, optional): HTTP headers to include in the request.

    Returns:
        dict: Parsed JSON response.

    Raises:
        requests.RequestException: If the HTTP request fails.
    """
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch JSON from {url}: {e}")
        raise


def is_coordinate_within_bbox(x, y, bbox):
    """
    Check if a coordinate (x, y) lies within a bounding box.

    Args:
        x (float): Latitude coordinate.
        y (float): Longitude coordinate.
        bbox (dict): Bounding box with keys 'minimum_latitude', 'maximum_latitude', 'minimum_longitude', 'maximum_longitude'.

    Returns:
        bool: True if coordinate is within bbox, False otherwise.
    """
    return (
        bbox["minimum_latitude"] <= x <= bbox["maximum_latitude"]
        and bbox["minimum_longitude"] <= y <= bbox["maximum_longitude"]
    )


def file_matches_criteria(file_data, provider, x, y):
    """
    Determine if a file's metadata matches given provider and/or coordinate criteria.

    Args:
        file_data (dict): Metadata of the file.
        provider (str): Provider name to match.
        x (float): Latitude coordinate.
        y (float): Longitude coordinate.

    Returns:
        bool: True if file matches criteria, False otherwise.
    """
    bbox = file_data.get("location", {}).get("bounding_box")
    if x and y and bbox:
        return is_coordinate_within_bbox(x, y, bbox)
    if provider:
        return provider.lower() in file_data.get("provider", "").lower()
    return False


def process_file(file, headers, city_files, provider, x, y):
    """
    Process a file's metadata and determine if it should be included based on criteria.

    Args:
        file (dict): File metadata from catalog.
        headers (dict): HTTP headers for requests.
        city_files (list): List of city-specific files.
        provider (str): Provider name to match.
        x (float): Latitude coordinate.
        y (float): Longitude coordinate.

    Returns:
        dict or None: File data if it matches criteria, None otherwise.
    """
    try:
        file_meta_url = f"{CATALOG_SCHEDULE_URL}/{file['name']}"
        git_file_data = fetch_json(file_meta_url, headers=headers)
        file_url = git_file_data.get("download_url")

        if not file_url:
            logger.warning(f"No download URL found for file {file['name']}")
            return None

        file_data = fetch_json(file_url, headers=headers)

        if not city_files and not file_matches_criteria(file_data, provider, x, y):
            return None

        return file_data
    except Exception as e:
        logger.error(f"Error processing file {file['name']}: {e}")
        return None


def fetch_and_filter_files(country_code, city=None, provider=None, x=None, y=None):
    """
    Fetch and filter files from the catalog based on country code, city, provider, and coordinates.

    Args:
        country_code (str): Country code to filter files.
        city (str, optional): City name to filter files.
        provider (str, optional): Provider name to filter files.
        x (float, optional): Latitude coordinate.
        y (float, optional): Longitude coordinate.

    Returns:
        list: List of filtered file metadata dictionaries.
    """
    headers = get_auth_headers()
    files = fetch_json(CATALOG_SCHEDULE_URL, headers=headers)

    country_files = [f for f in files if f["name"].startswith(f"{country_code}-")]
    city_files = [f for f in country_files if city and city.lower() in f["name"].lower()]
    target_files = city_files if city_files else country_files

    if not target_files:
        logger.warning(f"No files found for country code '{country_code}' and city '{city}'.")
        return []

    matching_files = []
    for file in target_files:
        result = process_file(file, headers, city_files, provider, x, y)
        if result:
            matching_files.append(result)

    return matching_files


def download_and_yield_rows(data_entry):
    """
    Download a ZIP archive from the latest URL in data_entry and yield rows from contained text files.

    Args:
        data_entry (dict): Data entry containing URLs.

    Yields:
        dict: Rows from CSV files inside the ZIP archive, marked with table names.
    """
    latest_url = data_entry.get("urls", {}).get("latest")
    if not latest_url:
        return

    logger.info(f"Processing URL: {latest_url}")
    response = requests.get(latest_url, stream=True, timeout=60)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        for name in archive.namelist():
            if name.endswith(".txt"):
                with archive.open(name) as f:
                    reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
                    for row in reader:
                        row["source_url"] = latest_url
                        row["source_file"] = name
                        #table_name = f"{os.path.splitext(os.path.basename(latest_url))[0]}_{name}"
                        table_name = name.replace('.txt', '')

                        yield dlt.mark.with_table_name(row, table_name=table_name)


def get_data(country_code, city, gtfs_type, provider=None, x_coordinate=None, y_coordinate=None):
    """
    Get GTFS data for a specific country and city, filtered by provider and coordinates.

    Args:
        country_code (str): Country code.
        city (str): City name.
        gtfs_type (str): GTFS data type.
        provider (str, optional): Provider name.
        x_coordinate (float, optional): Latitude coordinate.
        y_coordinate (float, optional): Longitude coordinate.

    Yields:
        dict: Rows of GTFS data.
    """
    info_data = fetch_and_filter_files(
        country_code=country_code,
        city=city,
        provider=provider,
        x=x_coordinate,
        y=y_coordinate
    )

    if not info_data:
        error_msg = f"No data found for country code: {country_code}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    for data in info_data:
        yield from download_and_yield_rows(data)
