import dlt
import os
import json
import argparse
from jsonschema import validate, ValidationError
from cm_data_ingestion.sources.openstreetmap import osm_resource
from cm_data_ingestion.sources.schema.config_schema import config_schema


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
            validate(config, schema=config_schema)
            return config
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {config_path}")
        exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Could not decode JSON from configuration file: {e}")
        exit(1)
    except ValidationError as e:
        print(f"Configuration validation error: {e}")
        exit(1)
    except ValueError as e:
        print(f"Error: Invalid configuration: {e}")
        exit(1)


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