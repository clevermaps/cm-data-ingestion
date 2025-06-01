import os
import requests
import duckdb
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


def get_data(country_code, tag, value, release, element_type=None):
    country_data = get_country_by_iso_code(country_code)

    if not country_data:
        raise ValueError(f"No data found for ISO code: {country_code}")

    url = country_data.get("urls", {}).get("pbf")

    if not url:
        raise ValueError(f"No PBF URL found for ISO code: {country_code}")

    # Step 1: Download the PBF file
    pbf_file_name = f"{country_code}_data.pbf"
    pbf_file_path = os.path.join(TEMP_DIR, pbf_file_name)
    download_pbf(url, pbf_file_path)

    # Step 2: Process the PBF file using DuckDB
    rows, column_names = process_pbf_with_duckdb(pbf_file_path, tag, value, element_type)

    # Step 3: Yield the results as dictionaries
    total_nodes = len(rows)
    print(f"Total items fetched: {total_nodes}")

    for index, row in enumerate(rows):
        if (index + 1) % 100 == 0 or index + 1 == total_nodes:
            print(f"Processed {index + 1}/{total_nodes} items")

        yield dict(zip(column_names, row))