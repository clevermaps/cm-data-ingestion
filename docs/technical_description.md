# Technical Description of the cm-data-ingestion Repository

## Overview

The `cm-data-ingestion` repository is designed to facilitate the ingestion, processing, and management of various geospatial and transit data sources. It is structured into several key components including sources, pipelines, and helpers, enabling modular and extensible data workflows.

## Architecture

### Python dlt sources

- **Sources**: Located in `src/cm_data_ingestion/sources.py`, this directory contains modules responsible for fetching and processing data from different providers such as Geoboundaries, GTFS (General Transit Feed Specification), OpenStreetMap, OvertureMaps, and WorldPop. Each source module encapsulates the logic specific to its data format and API.

- **Pipelines**: Found in `src/cm_data_ingestion/pipelines.py`, prepared pipelines that orchestrate the data ingestion workflows. They manage the sequence of operations, including data extraction and loading to the some common destinations such as DuckDb, filesystem or cloud storage. You can easily write your own pipelines in the standard dlt way using any supported destination.

- **Helpers**: Found in `src/cm_data_ingestion/helpers.py` Utility functions and shared logic are organized under helpers within both sources and pipelines. These include common data processing routines, API interaction helpers, and configuration management.

### Dbt stating models

Located in `dbt` folder, prepared dbt staging models with basic transformation logic. Each source has its own separate dbt project.

## Data Flow

1. **Configuration**: Users define ingestion configurations specifying providers, data items, and options.

2. **Data Extraction**: Source modules fetch raw data from external APIs or files, handling authentication, downloading, and initial parsing.

3. **Loading**: Processed data is loaded into DuckDB databases or other destinations for downstream use.

4. **Staging normalization**: Optionally the raw data can be transformed into the staging models in downstream dbt project.

## Key Technologies

- Python 3 for core logic and scripting.
- DuckDB for embedded analytical database capabilities.
- PyArrow and related libraries for efficient data handling.
- Requests and other HTTP libraries for API communication.
- Pytest for unit testing.

## Extensibility

The modular design allows easy addition of new data sources or pipelines by adhering to established interfaces and patterns.

## Examples

Example configurations and usage can be found in `example.py`, demonstrating how to set up ingestion for various sources.

---

