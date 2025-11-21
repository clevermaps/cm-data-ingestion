
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