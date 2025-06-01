import dlt
import os
import json

from cm_data_ingestion.sources.openstreetmap import osm_resource


def load_config(config_path):
    """Loads the JSON configuration file."""
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config


def validate_config(config):
    """Validates the loaded configuration."""
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a dictionary.")

    if 'database_path' not in config:
        raise ValueError("Missing 'database_path' in configuration.")
    if not isinstance(config['database_path'], str):
        raise ValueError("'database_path' must be a string.")

    if 'downloads' not in config:
        raise ValueError("Missing 'downloads' list in configuration.")
    if not isinstance(config['downloads'], list):
        raise ValueError("'downloads' must be a list.")

    # Keys that are always required for each download item, with their expected types
    always_required_keys_with_type = {
        'country_code': str,
        'table_name': str
    }
    # Keys that are optional but have type constraints if present
    optional_keys_with_type = {
        'tag': str,
        'release': str,
        'element_type': str  # element_type can also be None
    }

    for i, item in enumerate(config['downloads']):
        if not isinstance(item, dict):
            raise ValueError(f"Download item at index {i} must be a dictionary.")

        for key, expected_type in always_required_keys_with_type.items():
            if key not in item:
                raise ValueError(f"Missing key '{key}' in download item at index {i}.")
            if not isinstance(item[key], expected_type):
                raise ValueError(
                    f"Key '{key}' in download item at index {i} must be of type {expected_type.__name__}.")

        for key, expected_type in optional_keys_with_type.items():
            if key in item and item[key] is not None and not isinstance(item[key], expected_type):
                raise ValueError(
                    f"Optional key '{key}' in download item at index {i} must be a {expected_type.__name__} or null.")

        # Validate 'value'
        if 'value' in item and item['value'] is not None:
            if 'tag' not in item or item['tag'] is None:
                raise ValueError(
                    f"If 'value' is present in download item at index {i}, 'tag' must also be present and not null.")
            # 'value' itself can be of various types or null, so no strict type check here beyond its dependency on 'tag'

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
            release=config_item['release'],
            element_type=config_item.get('element_type')  # Use .get() for optional keys
        )
        result = pipeline.run(
            resource,
            table_name=config_item['table_name']
        )
        print(result)


if __name__ == "__main__":
    # Define paths and names
    current_working_directory = os.getcwd()
    config_file_path = os.path.join(current_working_directory, 'config.json')

    # Load configuration
    try:
        configuration = load_config(config_file_path)
        validate_config(configuration)
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {config_file_path}")
        exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Could not decode JSON from configuration file: {e}")
        exit(1)
    except ValueError as e:
        print(f"Error: Invalid configuration: {e}")
        exit(1)

    db_path_from_config = configuration['database_path']
    # If db_path_from_config is relative, join with CWD, otherwise use as is.
    if not os.path.isabs(db_path_from_config):
        db_path = os.path.join(current_working_directory, db_path_from_config)
    else:
        db_path = db_path_from_config

    downloads = configuration.get('downloads', [])

    if not downloads:
        print("No download configurations found in the config file.")
    else:
        # Run the pipeline with the loaded configurations
        run_osm_pipeline(
            pipeline_name="osm",
            destination_path=db_path,
            dataset_name='osm',
            download_configs=downloads
        )