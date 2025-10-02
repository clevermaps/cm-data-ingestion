import argparse
import json
import logging
import os
from typing import Any, Dict, List

import dlt
from cm_data_ingestion.sources.gtfs.transit import transit_resource

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def parse_args() -> str:
    """Parse CLI arguments and return the config file path."""
    parser = argparse.ArgumentParser(description="GTFS data pipeline")
    parser.add_argument(
        "--config", "-c", required=True, help="Path to the configuration file"
    )
    args = parser.parse_args()

    return os.path.join(os.getcwd(), args.config)


def load_config(config_path: str) -> Dict[str, Any]:
    """Load and validate JSON configuration."""
    if not os.path.exists(config_path):
        logging.error("Configuration file not found: %s", config_path)
        exit(1)

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        logging.error("Invalid JSON in config file: %s", e)
        exit(1)

    try:
        validate_config(config)
    except ValueError as e:
        logging.error("Invalid configuration: %s", e)
        exit(1)

    return config


def validate_config(config: Dict[str, Any]) -> None:
    """Validate configuration structure and types."""
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a dictionary.")

    # Required top-level keys
    if not isinstance(config.get("database_path"), str):
        raise ValueError("'database_path' must be a string.")

    downloads = config.get("downloads")
    if not isinstance(downloads, list):
        raise ValueError("'downloads' must be a list.")

    # Per-download validation
    always_required = {"country_code": str, "gtfs_type": str}
    optional_fields = {
        "city": str,
        "provider": str,
        "transit_type": str,
        "x-coordinate": float,
        "y-coordinate": float,
    }

    for i, item in enumerate(downloads):
        if not isinstance(item, dict):
            raise ValueError(f"Download item {i} must be a dictionary.")

        for key, typ in always_required.items():
            if not isinstance(item.get(key), typ):
                raise ValueError(
                    f"Key '{key}' in download item {i} must be of type {typ.__name__}."
                )

        for key, typ in optional_fields.items():
            if key in item and item[key] is not None and not isinstance(item[key], typ):
                raise ValueError(
                    f"Optional key '{key}' in download item {i} must be {typ.__name__} or null."
                )

        if "value" in item and item["value"] is not None:
            if not item.get("tag"):
                raise ValueError(
                    f"If 'value' is set in item {i}, 'tag' must also be provided."
                )

    logging.info("Configuration validated successfully.")


def run_osm_pipeline(
        pipeline_name: str,
        destination_path: str,
        dataset_name: str,
        download_configs: List[Dict[str, Any]],
) -> None:
    """Run the DLT pipeline for each download configuration."""
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.duckdb(destination_path),
        dataset_name=dataset_name,
    )

    for config_item in download_configs:
        logging.info("Processing transit feed for %s", config_item["country_code"])
        resource = transit_resource(
            country_code=config_item["country_code"],
            city=config_item.get("city"),
            gtfs_type=config_item["gtfs_type"],
            provider=config_item.get("provider"),
            x_coordinate=config_item.get("x-coordinate"),
            y_coordinate=config_item.get("y-coordinate"),
        )
        result = pipeline.run(resource, write_disposition="replace")
        logging.info("Pipeline result: %s", result)


def main() -> None:
    config_path = parse_args()
    config = load_config(config_path)

    db_path = (
        os.path.join(os.getcwd(), config["database_path"])
        if not os.path.isabs(config["database_path"])
        else config["database_path"]
    )

    downloads = config.get("downloads", [])
    if not downloads:
        logging.warning("No download configurations found in the config file.")
        return

    run_osm_pipeline(
        pipeline_name="transit",
        destination_path=db_path,
        dataset_name="transit",
        download_configs=downloads,
    )


if __name__ == "__main__":
    main()
