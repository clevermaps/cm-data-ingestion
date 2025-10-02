import os
import json
import argparse
import dlt

from cm_data_ingestion.sources.gtfs.mobilitydatabase import mobility_database_resource


def get_config_path():
    """Parses the CLI argument and returns the absolute path to the configuration file."""
    parser = argparse.ArgumentParser(description="GTFS data pipeline")
    parser.add_argument(
        "--config", "-c", required=True, help="Path to the configuration file"
    )
    args = parser.parse_args()

    return os.path.abspath(os.path.join(os.getcwd(), args.config))


def load_config():
    """Loads and validates the JSON configuration file."""
    config_path = get_config_path()

    try:
        with open(config_path, "r") as f:
            config = json.load(f)
            validate_config(config)
            return config
    except FileNotFoundError:
        exit_with_error(f"Configuration file not found at: {config_path}")
    except json.JSONDecodeError as e:
        exit_with_error(f"Invalid JSON in configuration file: {e}")
    except ValueError as e:
        exit_with_error(f"Configuration validation error: {e}")


def validate_config(config):
    """Validates the structure and content of the configuration dictionary."""
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a dictionary.")

    if "database_path" not in config or not isinstance(config["database_path"], str):
        raise ValueError("Missing or invalid 'database_path' in configuration.")

    downloads = config.get("downloads")
    if not isinstance(downloads, list):
        raise ValueError("'downloads' must be a list.")

    required_keys = {
        "country_code": str,
        "gtfs_type": str,
    }

    optional_keys = {
        "city": str,
        "provider": str,
        "transit_type": str,
        "x-coordinate": float,
        "y-coordinate": float,
    }

    for idx, item in enumerate(downloads):
        if not isinstance(item, dict):
            raise ValueError(f"Download item at index {idx} must be a dictionary.")

        for key, expected_type in required_keys.items():
            if key not in item:
                raise ValueError(f"Missing key '{key}' in download item at index {idx}.")
            if not isinstance(item[key], expected_type):
                raise ValueError(
                    f"Key '{key}' in download item at index {idx} must be of type {expected_type.__name__}."
                )

        for key, expected_type in optional_keys.items():
            if key in item and item[key] is not None and not isinstance(item[key], expected_type):
                raise ValueError(
                    f"Optional key '{key}' in download item at index {idx} must be of type {expected_type.__name__} or None."
                )

        if "value" in item and item["value"] is not None:
            if "tag" not in item or item["tag"] is None:
                raise ValueError(
                    f"If 'value' is present in item {idx}, 'tag' must also be present and not None."
                )

    print("Configuration validated successfully.")


def run_osm_pipeline(pipeline_name, destination_path, dataset_name, download_configs):
    """
    Runs the DLT pipeline for each download configuration.

    Args:
        pipeline_name (str): Name of the DLT pipeline.
        destination_path (str): Path to the DuckDB destination file.
        dataset_name (str): Name of the dataset.
        download_configs (list): List of download configuration dictionaries.
    """
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.duckdb(destination_path),
        dataset_name=dataset_name,
    )

    for config_item in download_configs:
        print("-------")
        print("Processing download for Mobility Schedule...")

        resource = mobility_database_resource(
            country_code=config_item["country_code"],
            city=config_item.get("city"),
            gtfs_type=config_item["gtfs_type"],
            provider=config_item.get("provider"),
            x_coordinate=config_item.get("x-coordinate"),
            y_coordinate=config_item.get("y-coordinate"),
        )

        result = pipeline.run(resource, write_disposition="replace")
        print(result)


def exit_with_error(message):
    """Exits the script with an error message."""
    print(f"Error: {message}")
    exit(1)


def resolve_database_path(path):
    """Resolves an absolute path for the database."""
    return os.path.abspath(os.path.join(os.getcwd(), path)) if not os.path.isabs(path) else path


if __name__ == "__main__":
    config = load_config()

    db_path = resolve_database_path(config["database_path"])
    downloads = config.get("downloads", [])

    if not downloads:
        print("No download configurations found in the config file.")
    else:
        run_osm_pipeline(
            pipeline_name="mobility",
            destination_path=db_path,
            dataset_name="mobility",
            download_configs=downloads,
        )
