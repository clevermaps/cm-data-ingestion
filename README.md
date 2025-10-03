# CleverMaps Data Ingestion

The `cm-data-ingestion` package provides easy data ingestion from various geodata sources. This package is based on the great `dlt` framework (https://dlthub.com/), so all custom sources can be easily pipelined into the standard destinations supported by `dlt` (https://dlthub.com/docs/dlt-ecosystem/destinations/). The main idea behind this package is to break siloed geodata and make it easily accessible in standard data environments for further visualization or analytics. The package is also built with the `analytics as code` approach in mind.

Currently supported sources:
* OvertureMaps
* OpenStreetMap
* WorldPop
* GTFS

## Installation

Run the following command to install the `cm-data-ingestion` package on your system:

```bash
    pip install git+https://github.com/clevermaps/cm-data-ingestion.git
```

## Example

Check `pipelines` folder.

## Changelog

0.0.1 Initial version


## Running OVM pipeline

TODO

## Running OSM pipeline

Configure download settings in `config.json`.

Init virtual env, install all deps and run OSM pipeline.

```bash
python -m venv venv
source venv/bin/activate
pip install .
pip install -r src/cm_data_ingestion/sources/openstreetmap/requirements.txt
python pipelines/osm_pipeline.py -c config.json
```

DuckDB with downloaded data is located at `openstreetmap.db`. Check it with DuckDB CLI:

```bash
duckdb openstreetmap.db
```

or using DuckDB UI:

```bash
duckdb --ui openstreetmap.db
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



## Running GTFS Mobility pipeline

Configure download settings in `mobilityConfig.json`.

`GITHUB_TOKEN` is needed to avoid rate limiting when downloading data from GitHub.

Init virtual env, install all deps and run OSM pipeline.
```
python -m venv venv
source venv/bin/activate
pip install .
pip install -r src/cm_data_ingestion/sources/mobilitydatabase/requirements.txt
export GITHUB_TOKEN="XXXXXXXXXXXXXXXXXXXXXX"
python pipelines/mobility_pipeline.py -c mobilityConfig.json
```

DuckDB with downloaded data is located at `gtfs-mobility-scheduled.db`.

## Running GTFS Transit pipeline

Configure download settings in `transitConfig.json`.

`API_TOKEN` is needed to access Transit API. 
It can be generated after creating an account on https://transit.land/.

Init virtual env, install all deps and run OSM pipeline.
```
python -m venv venv
source venv/bin/activate
pip install .
pip install -r src/cm_data_ingestion/sources/mobilitydatabase/requirements.txt
export API_TOKEN="XXXXXXXXXXXXXXXXXXXXXX"
python pipelines/mobility_pipeline.py -c transitConfig.json
```

DuckDB with downloaded data is located at `gtfs-transit-scheduled.db`.

# How ingestion works

## Mobility Database

Everything is located in the Git repository. For local processing, the app is using the following URL to search in catalog:
https://github.com/MobilityData/mobility-database-catalogs.git


In the directory: `mobility-database-catalogs/catalogs/sources/gtfs`, it can be chosen whether you want to work with **Realtime** or **Schedule**.
We selected to use scheduled data.

In each directory, there are JSON files for individual transport systems. The description of the meaning of each part of the JSON file can be found here:  
[Mobility Database Schemas](https://github.com/MobilityData/mobility-database-catalogs/tree/main/schemas)

If you want to download data based on geolocation, the app goes through individual JSON files and look into the attribute `location.bounding_box` to check whether your coordinates are within the defined area.  
You can search by country. In `location.country_code`, there is the country code abbreviation.  
In `subdivision_name`, there is a more detailed description, but it is not necessarily a city. This attribute is available only in **Schedule**. Unfortunately. 
However, the country can be identified in both cases based on the prefix in the file name.

There is a download link in the `urls.direct_download` attribute.
- In **Schedule**, the link points to a ZIP file.

## Transit Database

This API uses the same approach as Mobility Database. The difference is that the data is obtained from https://transit.land/api/v2/rest/feeds?bbox=16.50,49.15,16.75,49.30&apikey=XXXXXXXX