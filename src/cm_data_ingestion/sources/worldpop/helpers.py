import os
import json
import urllib.request
import zipfile
import py7zr
import pandas as pd
import numpy as np
import rioxarray
from datetime import datetime
import dlt

from .settings import TEMP_DIR

def current_timestamp():
    return datetime.now().strftime("%Y/%m/%d %H:%M")

def download_file(url, output_path):
    try:
        print(f"Downloading {output_path}...")
        urllib.request.urlretrieve(url, output_path)
        print("Downloaded successfully.")
    except urllib.error.URLError as e:
        print(f"[ERROR] Failed to download {url}: {e}")
        return False
    return True

def extract_archive(archive_path, extract_to):
    print(f"Extracting {archive_path}...")
    try:
        if archive_path.endswith(".zip"):
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
        elif archive_path.endswith(".7z"):
            with py7zr.SevenZipFile(archive_path, mode='r') as archive:
                archive.extractall(path=extract_to)
        else:
            print(f"[ERROR] Unsupported archive format: {archive_path}")
            return None
        print("Extraction complete.")
        return extract_to
    except Exception as e:
        print(f"[ERROR] Failed to extract archive: {e}")
        return None

def find_tif_files(folder):
    return [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if f.lower().endswith((".tif", ".tiff"))
    ]

def convert_to_points(tif_path, db_name):
    try:
        print(f"Reading {tif_path} with rioxarray...")
        da = rioxarray.open_rasterio(tif_path, masked=True)
        da = da.squeeze()

        df = da.to_dataframe(name="value").reset_index()
        coord_names = da.coords
        x_name = next((n for n in coord_names if "x" in n.lower()), None)
        y_name = next((n for n in coord_names if "y" in n.lower()), None)

        if not x_name or not y_name:
            print(f"[ERROR] Couldn't identify x/y coordinate names in {tif_path}")
            return None

        df = df.rename(columns={x_name: "lon", y_name: "lat"})
        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["value"])
        df = df[df["value"] != 9999]
        df = df[df["value"] > -1e+30]
        df["name"] = db_name
        df["date"] = current_timestamp()

        return df[["name", "lon", "lat", "value", "date"]]
    except Exception as e:
        print(f"[ERROR] Failed to convert {tif_path} to points: {e}")
        return None

def make_raster_resource(json_path):
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"JSON file not found: {json_path}")

    with open(json_path, "r") as f:
        raster_info = json.load(f)

    @dlt.resource(name="raster_data")
    def raster_to_points():
        for entry in raster_info["data"]:
            url = entry["url"]
            base_name = os.path.basename(url)
            archive_path = os.path.join(TEMP_DIR, base_name)
            user_name = entry["name"]

            if not os.path.exists(archive_path):
                if not download_file(url, archive_path) is True:
                    continue

            if base_name.lower().endswith((".tif", ".tiff")):
                tif_files = [archive_path]
                name_source = "json"
            elif base_name.lower().endswith((".zip", ".7z")):
                extract_path = os.path.join(TEMP_DIR, base_name + "_extract")
                os.makedirs(extract_path, exist_ok=True)
                if extract_archive(archive_path, extract_path) is None:
                    continue
                tif_files = find_tif_files(extract_path)
                if not tif_files:
                    print(f"[ERROR] No .tif files found in archive: {base_name}")
                    continue
                name_source = "tif" if len(tif_files) > 1 else "json"
            else:
                print(f"[ERROR] Unsupported file type: {base_name}")
                continue

            for tif_path in tif_files:
                tif_filename = os.path.basename(tif_path)
                db_name = tif_filename if name_source == "tif" else user_name
                df = convert_to_points(tif_path, db_name)
                if df is None:
                    continue
                for row in df.itertuples(index=False):
                    yield {
                        "name": row.name,
                        "lon": row.lon,
                        "lat": row.lat,
                        "value": row.value,
                        "date": row.date
                    }

    return raster_to_points
