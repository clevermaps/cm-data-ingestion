import os
import requests
import duckdb
import re
from datetime import datetime
from bs4 import BeautifulSoup
from cm_data_ingestion.settings import TEMP_DIR
from cm_data_ingestion.sources.openstreetmap.settings import GEOFABRIK_INDEX_URL, GEOFABRIK_BASE_URL


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


def download_pbf(url, output_path, force=False):
    if force or not os.path.exists(output_path):
        if force and os.path.exists(output_path):
            print(f"Force downloading PBF file from {url} to {output_path}, overwriting existing file.")
        else:
            print(f"Downloading PBF file from {url} to {output_path}")

        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad HTTP responses
        with open(output_path, "wb") as file:
            file.write(response.content)
        print(f"Downloaded PBF file to {output_path}")
    else:
        print(f"PBF file already exists at {output_path}, skipping download.")


def setup_duckdb_extensions(con):
    con.execute("INSTALL 'spatial';")
    con.execute("LOAD 'spatial';")


def process_pbf_with_duckdb(pbf_file_path, tag=None, value=None, element_type=None):
    con = duckdb.connect()
    setup_duckdb_extensions(con)

    # Build the query dynamically based on the presence of tag, value, and element_type
    query = f"SELECT * FROM ST_ReadOsm('{pbf_file_path}')"
    conditions = []
    if tag:
        if value:
            # Filter by tag and its specific value
            conditions.append(f"json_extract_string(tags, '$.{tag}') = '{value}'")
        else:
            # Filter by the existence of the tag
            conditions.append(f"json_extract_string(tags, '$.{tag}') IS NOT NULL")

    if element_type:
        conditions.append(f"kind = '{element_type.lower()}'")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    result = con.execute(query)
    column_names = [desc[0] for desc in result.description]  # Get column names
    rows = result.fetchall()
    con.close()
    return rows, column_names

def get_historical_file_url(pbf_url, country_id, target_date):
    """
    Find historical OSM file closest to the target date.

    Args:
        pbf_url (str): URL to the latest PBF file
        country_id (str): Country ID
        target_date (str or datetime): Target date

    Returns:
        tuple: (URL to the historical file, actual date string from filename) or (None, None) if not found
    """
    # Extract the directory from the URL
    directory_url = os.path.dirname(pbf_url)

    # Convert target_date to datetime, removing .0 suffix if present
    if isinstance(target_date, str):
        target_date = target_date.split('.')[0]  # Remove decimal part if present
        target_date = datetime.strptime(target_date, "%Y-%m-%d")

    # Get the directory listing
    response = requests.get(directory_url)
    if response.status_code != 200:
        return None, None

    # Parse HTML to find available historical files
    soup = BeautifulSoup(response.content, 'html.parser')
    available_dates = []

    # Look for links with pattern country-YYMMDD.osm.pbf
    date_pattern = re.compile(rf'{os.path.basename(country_id)}-(\d{{6}})\.osm\.pbf')
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and date_pattern.search(href):
            date_str = date_pattern.search(href).group(1)
            try:
                # Convert YYMMDD to YYYYMMDD
                year_prefix = '20' if int(date_str[:2]) < 50 else '19'
                full_date_str = f"{year_prefix}{date_str}"
                date = datetime.strptime(full_date_str, "%Y%m%d")
                available_dates.append((date, href, date_str))
            except ValueError:
                continue

    if not available_dates:
        return None, None

    # Find the date closest to target_date
    closest_date = min(available_dates, key=lambda x: abs(x[0] - target_date))
    return f"{directory_url}/{closest_date[1]}", closest_date[2]

def get_data(country_code, tag, value, element_type=None, target_date=None):
    """
    Get OSM data for a specific country, filtered by tags and optionally by date.
    """
    country_data = get_country_by_iso_code(country_code)
    if not country_data:
        raise ValueError(f"No data found for ISO code: {country_code}")

    pbf_url = country_data.get("urls", {}).get("pbf")
    if not pbf_url:
        raise ValueError(f"No PBF URL found for ISO code: {country_code}")

    date_suffix = "latest"

    if target_date:
        historical_pbf_url, actual_date_str = get_historical_file_url(pbf_url, country_data['id'], target_date)
        if historical_pbf_url:
            pbf_url = historical_pbf_url
            # Use the actual date from filename instead of target date
            date_suffix = actual_date_str
            print(f"Found historical data for {country_code} near date: {target_date}. Using URL: {pbf_url}")
        else:
            print(f"No historical data found for {country_code} near date: {target_date}. Using latest data instead.")
    else:
        print(f"Using latest data from URL: {pbf_url}")

    # Step 1: Download the PBF file
    pbf_file_name = f"{country_code}_{date_suffix}_data.pbf"
    pbf_file_path = os.path.join(TEMP_DIR, pbf_file_name)
    download_pbf(pbf_url, pbf_file_path)

    # Process the rest as before
    rows, column_names = process_pbf_with_duckdb(pbf_file_path, tag, value, element_type)
    total_nodes = len(rows)
    print(f"Total items fetched: {total_nodes}")

    for index, row in enumerate(rows):
        if (index + 1) % 100 == 0 or index + 1 == total_nodes:
            print(f"Processed {index + 1}/{total_nodes} items")

        yield dict(zip(column_names, row))