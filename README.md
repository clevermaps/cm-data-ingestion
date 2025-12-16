# CleverMaps Data Ingestion

The `cm-data-ingestion` package provides easy data ingestion from various geodata sources. This package is based on the great `dlt` framework (https://dlthub.com/), so all custom sources can be easily pipelined into the standard destinations supported by `dlt` (https://dlthub.com/docs/dlt-ecosystem/destinations/). The main idea behind this package is to break siloed geodata and make it easily accessible in standard data environments for further visualization or analytics. The package is also built with the `analytics as code` approach in mind.

Currently supported Sources:
* OvertureMaps
* OpenStreetMap
* WorldPop
* GTFS
* GeoBoundaries

Using these Sources, you can ingest geodata from heterogeneous systems through a single, standardized ingestion logic, consistent with all other sources.

Prepared dbt staging models are also available in the `dbt` folder. These models perform basic normalization of the raw data ingested from the Sources and can be optionally used as part of your downstream transformation pipeline.

## Installation

Run the following command to install the `cm-data-ingestion` package on your system:

```bash
    pip install git+https://github.com/clevermaps/cm-data-ingestion.git
```

If you want to add dbt staging models into your dbt project, just add these to your `packages.yml` or `dependencies.yml` file.

```
packages:
  - git: "https://github.com/clevermaps/cm-data-ingestion.git"
    subdirectory: "dbt/osm"
  - git: "https://github.com/clevermaps/cm-data-ingestion.git"
    subdirectory: "dbt/ovm"
  - git: "https://github.com/clevermaps/cm-data-ingestion.git"
    subdirectory: "dbt/gtfs"
  - git: "https://github.com/clevermaps/cm-data-ingestion.git"
    subdirectory: "dbt/worldpop"
  - git: "https://github.com/clevermaps/cm-data-ingestion.git"
    subdirectory: "dbt/geobnd"

```

After that you call the models using standard `dbt ref()` function, e.g. `ref('gtfs', 'stops')`.

## Documentation

More detailed documentation can be found in `docs` folder.

## Examples

Example configurations and usage can be found in `examples`, demonstrating how to set up ingestion for various sources.

## Key Technologies

- Python 3 for core logic and scripting.
- DuckDB for embedded analytical database capabilities.
- PyArrow and related libraries for efficient data handling.
- Requests and other HTTP libraries for API communication.
- Pytest for unit testing.

## Extensibility

The modular design allows easy addition of new data sources or pipelines by adhering to established interfaces and patterns.


# Changelog

0.0.1 Initial version






