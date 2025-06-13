import dlt
import os
import json
import argparse

from cm_data_ingestion.sources.openstreetmap import osm_resource


def get_config_path():
    parser = argparse.ArgumentParser(description='OpenStreetMap data pipeline')
    parser.add_argument('--config', '-c', required=True,
                        help='Path to the configuration file')
    args = parser.parse_args()

    current_working_directory = os.getcwd()
    config_file_path = os.path.join(current_working_directory, args.config)

    return config_file_path


def get_database_path(config):
    """Returns the database path from the configuration."""
    path = config['database_path']

    # If db_path_from_config is relative, join with CWD, otherwise use as is.
    if not os.path.isabs(path):
        path = os.path.join(os.getcwd(), path)

    return path


def load_config():
    """Loads the JSON configuration file."""

    config_path = get_config_path()

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            validate_config(config)
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {config_path}")
        exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Could not decode JSON from configuration file: {e}")
        exit(1)
    except ValueError as e:
        print(f"Error: Invalid configuration: {e}")
        exit(1)

    return config


def validate_config(config):
    """Validates the loaded configuration."""
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a dictionary.")

    # Required
    if 'database_path' not in config or config['database_path'] is None or not isinstance(config['database_path'], str):
        raise ValueError("Missing 'database_path' in configuration as str.")

    # Required
    if 'downloads' not in config or config['downloads'] is None or not isinstance(config['downloads'], list):
        raise ValueError("Missing 'downloads' list in configuration.")

    for i, item in enumerate(config['downloads']):
        if not isinstance(item, dict):
            raise ValueError(f"Download item at index {i} must be a dictionary.")

        # Required
        if 'country_code' not in item or item['country_code'] is None or not isinstance(item['country_code'], str):
            raise ValueError(f"Missing key 'country_code' in download item at index {i} with data type str.")

        # Optional
        if 'tag' in item and item['tag'] is not None and not isinstance(item['tag'], str):
            raise ValueError(f"Optional key 'tag' in download item at index {i} must be a str or null.")

        # 'value' is optional, but if present, 'tag' must also be present and not null
        # 'value' itself can be of various types or null, so no strict type check here beyond its dependency on 'tag'
        if 'value' in item and item['value'] is not None:
            if 'tag' not in item or item['tag'] is None:
                raise ValueError(f"If 'value' is present in download item at index {i}, 'tag' must also be present and not null.")

        # Optional
        if 'element_type' in item and item['element_type'] is not None and not isinstance(item['element_type'], str):
             raise ValueError(f"Optional key 'element_type' in download item at index {i} must be a str or null.")

        # Optional
        if 'target_date_range' in item and item['target_date_range'] is not None:
            if not isinstance(item['target_date_range'], list) or len(item['target_date_range']) != 2:
                raise ValueError(f"'target_date_range' in download item at index {i} must be a list with two string elements - min and max date.")
            for date in item['target_date_range']:
                if not isinstance(date, str):
                    raise ValueError(f"Each date in 'target_date_range' at index {i} must be a string.")

        # Optional
        if 'target_date_tolerance_days' in item and item['target_date_tolerance_days'] is not None and not isinstance(item['target_date_tolerance_days'], int):
                raise ValueError(f"Optional key 'target_date_tolerance_days' in download item at index {i} must be an int or null.")

        # Required
        if 'table_name' not in item or item['table_name'] is None or not isinstance(item['table_name'], str):
                raise ValueError(f"Missing key 'table_name' in download item at index {i} with data type str.")

    print("Configuration validated successfully.")
    return True


def run_osm_pipeline(pipeline_name, destination_path, dataset_name, download_configs):
    """
    Initializes and runs the DLT pipeline for each download configuration.

    Args:
        pipeline_name (str): The name of the DLT pipeline.
        destination_path (str): The path to the DuckDB destination.
        dataset_name (str): The name of the dataset in the destination.
        download_configs (list): A list of download configurations.
    """
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.duckdb(destination_path),
        dataset_name=dataset_name
    )

    for config_item in download_configs:
        print('-------')
        print(f"Processing download for table: {config_item['table_name']}")
        resource = osm_resource(
            country_code=config_item['country_code'],
            tag=config_item['tag'],
            value=config_item['value'],
            element_type=config_item.get('element_type', None),  # Use .get() for optional keys
            target_date=config_item.get('target_date', None)  # Default to None if not provided
        )
        result = pipeline.run(
            resource,
            table_name=config_item['table_name']
        )
        print(result)


if __name__ == "__main__":
    config = load_config()
    db_path = get_database_path(config)
    downloads = config.get('downloads', [])

    if not downloads:
        print("No download configurations found in the config file.")
        exit(1)

    # Run the pipeline with the loaded configurations
    run_osm_pipeline(
        pipeline_name="osm",
        destination_path=db_path,
        dataset_name='osm',
        download_configs=downloads
    )