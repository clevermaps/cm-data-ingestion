import csv
import io
import os
import zipfile
from typing import List, Dict, Optional, Generator

import dlt
import requests
from .settings import TRANSIT_FEED_URL, OSM_URL


def get_bounding_box(
        city: Optional[str] = None,
        x_coordinate: Optional[float] = None,
        y_coordinate: Optional[float] = None
) -> Optional[List[str]]:
    """
    Retrieve bounding box from OSM by city name or coordinates.
    """
    headers = {
        "User-Agent": "myapp/1.0 (your-email@example.com)"
    }

    if city:
        params = {"q": city, "format": "json"}
        url = f"{OSM_URL}/search"
    elif x_coordinate is not None and y_coordinate is not None:
        params = {"lat": x_coordinate, "lon": y_coordinate, "format": "json"}
        url = f"{OSM_URL}/reverse"
    else:
        return None

    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()

    data = response.json()
    if not data or len(data) < 2:
        return None

    return data[1].get("boundingbox")


def fetch_mobility_feeds(
        bbox: Optional[List[str]],
        provider: Optional[str] = None
) -> List[Dict[str, str]]:
    """
    Fetch GTFS feeds from the mobility API, filtered by bounding box and provider.
    """
    api_token = os.getenv("API_TOKEN")
    bbox_str = ",".join([bbox[2], bbox[0], bbox[3], bbox[1]]) if bbox else None

    params = {"apikey": api_token}
    if bbox_str:
        params["bbox"] = bbox_str

    feeds = []
    url = TRANSIT_FEED_URL

    while url:
        resp = requests.get(url, params=params if url == TRANSIT_FEED_URL else None)
        resp.raise_for_status()
        data = resp.json()

        for feed in data.get("feeds", []):
            if provider:
                if ("".join(provider.split())).lower() not in feed.get("onestop_id", "").lower():
                    continue
            feeds.append({
                "url": feed.get("urls", {}).get("static_current"),
                "onestop_id": feed.get("onestop_id")
            })

        meta = data.get("meta", {})
        url = meta.get("next")

    return feeds


def download_and_yield_gtfs_data(
        feeds: List[Dict[str, str]]
) -> Generator[dict, None, None]:
    """
    Download GTFS ZIP files from the provided feeds and yield their TXT contents as dicts.
    """
    for feed in feeds:
        feed_url = feed.get("url")
        if not feed_url:
            continue

        resp = requests.get(feed_url, stream=True)
        resp.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            for name in z.namelist():
                if name.endswith(".txt"):
                    with z.open(name) as f:
                        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
                        for row in reader:
                            row["source_url"] = feed_url
                            row["source_file"] = name
                            yield dlt.mark.with_table_name(
                                row, table_name=f"{feed['onestop_id']}_{name}"
                            )


def get_data(
        country_code: str,
        city: Optional[str],
        gtfs_type: str,
        provider: Optional[str] = None,
        x_coordinate: Optional[float] = None,
        y_coordinate: Optional[float] = None
) -> Generator[dict, None, None]:
    """
    Orchestrates bounding box retrieval, feed fetching, and GTFS data extraction.
    """
    bbox = get_bounding_box(city, x_coordinate, y_coordinate)
    if not bbox:
        raise ValueError(f"City '{city}' or coordinates ({x_coordinate}, {y_coordinate}) not found.")

    feeds = fetch_mobility_feeds(bbox, provider)
    if not feeds:
        raise ValueError(f"No data found for country code: {country_code}")

    yield from download_and_yield_gtfs_data(feeds)
