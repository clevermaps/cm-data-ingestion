# CleverMaps Data Ingestion

The `cm-data-ingestion` package provides easy data ingestion from various geodata sources. This package is based on the great `dlt` framework (https://dlthub.com/), so all custom sources can be easily pipelined into the standard destinations supported by `dlt` (https://dlthub.com/docs/dlt-ecosystem/destinations/). The main idea behind this package is to break siloed geodata and make it easily accessible in standard data environments for further visualization or analytics. The package is also built with the `analytics as code` approach in mind.

Currently supported sources:
* OvertureMaps

Planned sources:
* OpenStreetMap
* WorldPop

## Installation

Run the following command to install the `cm-data-ingestion` package on your system:

    pip install git+https://github.com/clevermaps/cm-data-ingestion.git

## Example

Check `pipelines` folder.

## Changelog

0.0.1 Initial version


## Running OSM pipeline

Configure download settings in `config.json`.

Init virtual env, install all deps and run OSM pipeline.
```
python -m venv venv
source venv/bin/activate
pip install .
pip install -r src/cm_data_ingestion/sources/openstreetmap/requirements.txt
python pipelines/osm_pipeline.py -c config.json
```

DuckDB with downloaded data is located at `openstreetmap.db`.

## Running Worldpop pipeline

Configure download settings in `config_worldpop.json`.

Init virtual env, install all deps and run OSM pipeline.
```
python -m venv venv
source venv/bin/activate
pip install .
pip install -r src/cm_data_ingestion/sources/worldpop/requirements.txt
python pipelines/worldpop_pipeline.py -c config_worldpop.json
```

DuckDB with downloaded data is located at `worldpop.duckdb`.