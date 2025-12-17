import os
import urllib.request
import zipfile
import py7zr
import numpy as np
import rioxarray
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def current_timestamp():
    """
    Get the current timestamp formatted as 'YYYY/MM/DD HH:MM'.

    Returns:
        str: Current timestamp string.
    """
    return datetime.now().strftime("%Y/%m/%d %H:%M")

def download_file(url, output_path):
    """
    Download a file from a URL to a specified output path.

    Args:
        url (str): URL of the file to download.
        output_path (str): Local path to save the downloaded file.

    Returns:
        bool: True if download succeeded, False otherwise.
    """
    try:
        logger.info(f"Downloading {output_path}...")
        urllib.request.urlretrieve(url, output_path)
        logger.info("Downloaded successfully.")
    except urllib.error.URLError as e:
        logger.error(f"Failed to download {url}: {e}")
        return False
    return True

def extract_archive(archive_path, extract_to):
    """
    Extract a ZIP or 7z archive to a specified directory.

    Args:
        archive_path (str): Path to the archive file.
        extract_to (str): Directory to extract files into.

    Returns:
        str or None: Path to extraction directory if successful, None otherwise.
    """
    logger.info(f"Extracting {archive_path}...")
    try:
        if archive_path.endswith(".zip"):
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
        elif archive_path.endswith(".7z"):
            with py7zr.SevenZipFile(archive_path, mode='r') as archive:
                archive.extractall(path=extract_to)
        else:
            logger.error(f"Unsupported archive format: {archive_path}")
            return None
        logger.info("Extraction complete.")
        return extract_to
    except Exception as e:
        logger.error(f"Failed to extract archive: {e}")
        return None

def find_tif_files(folder):
    """
    Find all .tif and .tiff files in a folder.

    Args:
        folder (str): Directory path to search.

    Returns:
        list: List of file paths matching .tif or .tiff extensions.
    """
    return [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if f.lower().endswith((".tif", ".tiff"))
    ]

def convert_to_points(tif_path, db_name):
    """
    Convert a TIFF raster file to a DataFrame of points with coordinates and values.

    Args:
        tif_path (str): Path to the TIFF file.
        db_name (str): Name to assign to the data points.

    Returns:
        pandas.DataFrame or None: DataFrame with columns ['name', 'lon', 'lat', 'value'] or None on failure.
    """
    try:
        logger.info(f"Reading {tif_path} with rioxarray...")
        da = rioxarray.open_rasterio(tif_path, masked=True)
        da = da.squeeze()

        df = da.to_dataframe(name="value").reset_index()
        coord_names = da.coords
        x_name = next((n for n in coord_names if "x" in n.lower()), None)
        y_name = next((n for n in coord_names if "y" in n.lower()), None)

        if not x_name or not y_name:
            logger.error(f"Couldn't identify x/y coordinate names in {tif_path}")
            return None

        df = df.rename(columns={x_name: "lon", y_name: "lat"})
        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["value"])
        df = df[df["value"] != 9999]
        df = df[df["value"] > -1e+30]
        df["name"] = db_name
        # df["date"] = current_timestamp()

        return df[["name", "lon", "lat", "value"]]
    
    except Exception as e:
        logger.error(f"Failed to convert {tif_path} to points: {e}")
        return None


def raster_to_points(country, theme, temp_dir):
    """
    Download and process raster data from a URL, yielding points with coordinates and values.

    Args:
        url (str): URL to the raster or archive file.
        file_name (str): Name to assign to data points if applicable.
        temp_dir (str): Temporary directory for downloads and extraction.

    Yields:
        dict: Dictionary with keys 'name', 'lon', 'lat', 'value' for each data point.
    """

    url = get_worldpop_url(country, theme)
    file_name = os.path.basename(url)

    base_name = os.path.basename(url)
    archive_path = os.path.join(temp_dir, base_name)

    if not os.path.exists(archive_path):
        if not download_file(url, archive_path) is True:
            return

    if base_name.lower().endswith((".tif", ".tiff")):
        tif_files = [archive_path]
        name_source = "json"
    elif base_name.lower().endswith((".zip", ".7z")):
        extract_path = os.path.join(temp_dir, base_name + "_extract")
        os.makedirs(extract_path, exist_ok=True)
        if extract_archive(archive_path, extract_path) is None:
            return
        tif_files = find_tif_files(extract_path)
        if not tif_files:
            logger.error(f"No .tif files found in archive: {base_name}")
            return
        name_source = "tif" if len(tif_files) > 1 else "json"
    else:
        logger.error(f"Unsupported file type: {base_name}")
        return

    for tif_path in tif_files:
        tif_filename = os.path.basename(tif_path)
        db_name = tif_filename if name_source == "tif" else file_name
        df = convert_to_points(tif_path, db_name)
        if df is None:
            continue
        
        for row in df.itertuples(index=False):
            yield {
                "name": row.name,
                "lon": row.lon,
                "lat": row.lat,
                "value": row.value
            }

def get_worldpop_url(country, theme):

    # TODO : add more themes
    if theme == 'population':
        url = 'https://data.worldpop.org/GIS/Population/Global_2015_2030/R2025A/2025/{}/v1/100m/constrained/{}_pop_2025_CN_100m_R2025A_v1.tif'.format(country.upper(), country)
    else:
        raise ValueError('Theme {} is not suported.'.format(theme))

    return url