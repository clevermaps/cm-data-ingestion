# CleverMaps Data Ingestion

The `cm-data-ingestion` package provides easy data ingestion from various geodata sources. This package is based on the great `dlt` framework (https://dlthub.com/), so all custom sources can be easily pipelined into the standard destinations supported by `dlt` (https://dlthub.com/docs/dlt-ecosystem/destinations/). The main idea behind this package is to break siloed geodata and make it easily accessible in standard data environments for further visualization or analytics. The package is also built with the `analytics as code` approach in mind.

Currently supported sources:
* OvertureMaps
* OpenStreetMap
* WorldPop
* GTFS
* GeoBoundaries

## Installation

Run the following command to install the `cm-data-ingestion` package on your system:

```bash
    pip install git+https://github.com/clevermaps/cm-data-ingestion.git
```

## Documentation

More detailed documentation can be found in `docs` folder.

## Examples

Example configurations and usage can be found in `example.py`, demonstrating how to set up ingestion for various sources.


## Technical overview

### Overview

The `cm-data-ingestion` repository is designed to facilitate the ingestion, processing, and management of various geospatial and transit data sources. It is structured into several key components including sources, pipelines, and helpers, enabling modular and extensible data workflows.

### Architecture

- **Sources**: Located in `src/cm_data_ingestion/sources`, this directory contains modules responsible for fetching and processing data from different providers such as Geoboundaries, GTFS (General Transit Feed Specification), OpenStreetMap, OvertureMaps, and WorldPop. Each source module encapsulates the logic specific to its data format and API.

- **Pipelines**: Found in `src/cm_data_ingestion/pipelines`, pipelines orchestrate the data ingestion workflows. They manage the sequence of operations, including data extraction, loading, and optionaly basic normalization transformations.

- **Helpers**: Utility functions and shared logic are organized under helpers within both sources and pipelines. These include common data processing routines, API interaction helpers, and configuration management.

### Data Flow

1. **Configuration**: Users define ingestion configurations specifying providers, data items, and options.

2. **Data Extraction**: Source modules fetch raw data from external APIs or files, handling authentication, downloading, and initial parsing.

3. **Loading**: Processed data is loaded into DuckDB databases or other destinations for downstream use.

4. **Transformation**: Optionaly, data is normalized using basic staging transformations defined as dbt models.


### Key Technologies

- Python 3 for core logic and scripting.
- DuckDB for embedded analytical database capabilities.
- PyArrow and related libraries for efficient data handling.
- Requests and other HTTP libraries for API communication.
- Pytest for unit testing.

### Extensibility

The modular design allows easy addition of new data sources or pipelines by adhering to established interfaces and patterns.


# Changelog

0.0.1 Initial version






