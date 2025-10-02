import os
import io
import csv
import zipfile
import requests
import dlt
import concurrent.futures
from .settings import CATALOG_SCHEDULE_URL


def get_auth_headers():
    token = os.getenv("GITHUB_TOKEN")
    return {"Authorization": f"token {token}"} if token else {}


def fetch_json(url, headers=None):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def is_coordinate_within_bbox(x, y, bbox):
    return (
            bbox["minimum_latitude"] <= x <= bbox["maximum_latitude"]
            and bbox["minimum_longitude"] <= y <= bbox["maximum_longitude"]
    )


def file_matches_criteria(file_data, provider, x, y):
    bbox = file_data.get("location", {}).get("bounding_box")
    if x and y and bbox:
        return is_coordinate_within_bbox(x, y, bbox)
    if provider:
        return provider.lower() in file_data.get("provider", "").lower()
    return False


def process_file(file, headers, city_files, provider, x, y):
    try:
        file_meta_url = f"{CATALOG_SCHEDULE_URL}/{file['name']}"
        git_file_data = fetch_json(file_meta_url, headers=headers)
        file_url = git_file_data.get("download_url")

        if not file_url:
            return None

        file_data = fetch_json(file_url, headers=headers)

        if not city_files and not file_matches_criteria(file_data, provider, x, y):
            return None

        return file_data
    except Exception as e:
        print(f"Error processing file {file['name']}: {e}")
        return None


def fetch_and_filter_files(country_code, city=None, provider=None, x=None, y=None):
    headers = get_auth_headers()
    files = fetch_json(CATALOG_SCHEDULE_URL, headers=headers)

    matching_files = []
    country_files = [f for f in files if f["name"].startswith(f"{country_code}-")]

    city_files = [f for f in country_files if city and city.lower() in f["name"].lower()]
    target_files = city_files if city_files else country_files

    if not target_files:
        print(f"No files found for country code '{country_code}' and city '{city}'.")
        return []

    # Parallelize file processing
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(process_file, file, headers, city_files, provider, x, y)
            for file in target_files
        ]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                matching_files.append(result)

    return matching_files


def download_and_yield_rows(data_entry):
    rows = []
    latest_url = data_entry.get("urls", {}).get("latest")
    if not latest_url:
        return rows

    try:
        print(f"Processing URL: {latest_url}")
        response = requests.get(latest_url)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            for name in archive.namelist():
                if name.endswith(".txt"):
                    with archive.open(name) as f:
                        content = f.read().decode("utf-8")
                        reader = csv.DictReader(io.StringIO(content))
                        for row in reader:
                            row["source_url"] = latest_url
                            row["source_file"] = name
                            table_name = f"{os.path.splitext(os.path.basename(latest_url))[0]}_{name}"
                            rows.append(dlt.mark.with_table_name(row, table_name=table_name))
    except Exception as e:
        print(f"Failed to process {latest_url}: {e}")

    return rows


def get_data(country_code, city, gtfs_type, provider=None, x_coordinate=None, y_coordinate=None):
    info_data = fetch_and_filter_files(
        country_code=country_code,
        city=city,
        provider=provider,
        x=x_coordinate,
        y=y_coordinate
    )

    if not info_data:
        raise ValueError(f"No data found for country code: {country_code}")

    # Parallelize data downloading and processing
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(download_and_yield_rows, data) for data in info_data]
        for future in concurrent.futures.as_completed(futures):
            for row in future.result():
                yield row
