import os
import requests
import duckdb
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
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

def get_available_historical_files(pbf_url, country_id):
    """
    Get a list of available historical OSM files based on the provided PBF URL and country ID.
    :param pbf_url: str
    :param country_id: str
    :return: list of tuples containing (date, file_url, date_str)
    """
    available_dates = []

    # Extract the directory from the URL
    directory_url = os.path.dirname(pbf_url)

    # Get the directory listing
    response = requests.get(directory_url)
    if response.status_code != 200:
        raise Exception(f"Failed to access directory: {directory_url} with status code {response.status_code}")

    # Parse HTML to find available historical files
    soup = BeautifulSoup(response.content, 'html.parser')

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
                available_dates.append((
                    date,
                    os.path.join(directory_url, href),
                    date_str
                ))
            except ValueError:
                continue

    return available_dates


def get_available_historical_files_in_range(pbf_url, country_id, target_date_range, target_date_tolerance_days=0):
    """
    Find historical OSM file URL based on the provided PBF URL and target date range.
    Args:
        pbf_url (str): The URL of the PBF file.
        country_id (str): The country ID to match in the file name.
        target_date_range (tuple): A tuple containing start and end dates as strings in 'YYYY-MM-DD' format.
        target_date_tolerance_days (int): Number of days to allow for tolerance in date matching.
    Returns:
        list: A list of tuples containing (date, file_url, date_str) for matching files.
    """

    # Convert target_date_range to datetime, removing .0 suffix if present
    start_date = target_date_range[0].split('.')[0]  # Remove decimal part if present
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = target_date_range[1].split('.')[0]  # Remove decimal part if present
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Get available historical files
    available_dates = get_available_historical_files(pbf_url, country_id)

    # Filter only relevant files within the target date range and tolerance
    filtered_available_dates = [
        (date, file_url, date_str) for date, file_url, date_str in available_dates
        if start_date - timedelta(days=target_date_tolerance_days) <= date <= end_date + timedelta(days=target_date_tolerance_days)
    ]

    return filtered_available_dates


def find_suitable_pbf_files(country_code, target_date_range=None, target_date_tolerance_days=0):
    country_data = get_country_by_iso_code(country_code)

    if not country_data:
        raise ValueError(f"No data found for ISO code: {country_code}")

    pbf_url = country_data.get("urls", {}).get("pbf")
    if not pbf_url:
        raise ValueError(f"No PBF URL found for ISO code: {country_code}")

    if target_date_range:
        available_files = get_available_historical_files_in_range(
            pbf_url,
            country_data['id'],
            target_date_range,
            target_date_tolerance_days
        )

        if len(available_files) == 0:
            raise ValueError(f"No suitable PBF file found for country code: {country_code} within the specified date range {target_date_range} and tolerance {target_date_tolerance_days} days.")

        return [(file_url, date_str) for date, file_url, date_str in available_files]

    return [(pbf_url, "latest")]

def find_most_recent_suitable_pbf_file(country_code, target_date_range=None, target_date_tolerance_days=0):
    files = find_suitable_pbf_files(
        country_code,
        target_date_range,
        target_date_tolerance_days
    )
    if len(files) == 0:
        return None, None
    else:
        return files[-1]


def get_data(country_code, tag, value, element_type=None, target_date_range=None, target_date_tolerance_days=0):
    """
    Get OSM data for a specific country, filtered by tags and optionally by date.
    """
    current_datetime = datetime.now().isoformat()  # Get the current date-time

    # Step 1: Find the suitable PBF file URL
    pbf_url, date_suffix = find_most_recent_suitable_pbf_file(country_code, target_date_range, target_date_tolerance_days)
    print('Using PBF URL:', pbf_url, 'with date suffix:', date_suffix)

    # Step 2: Download the PBF file
    pbf_file_name = f"{country_code}_{date_suffix}_data.pbf"
    pbf_file_path = os.path.join(TEMP_DIR, pbf_file_name)
    download_pbf(pbf_url, pbf_file_path)

    # Step 3: Process the PBF file with DuckDB
    rows, column_names = process_pbf_with_duckdb(pbf_file_path, tag, value, element_type)
    total_nodes = len(rows)
    print(f"Total items fetched: {total_nodes}")

    # Step 4: Add additional fields and yield the results
    for index, row in enumerate(rows):
        if (index + 1) % 100 == 0 or index + 1 == total_nodes:
            print(f"Processed {index + 1}/{total_nodes} items")

        # Add "data_version" and "imported_at" fields
        result = dict(zip(column_names, row))
        result["data_version"] = date_suffix
        result["imported_at"] = current_datetime

        yield result