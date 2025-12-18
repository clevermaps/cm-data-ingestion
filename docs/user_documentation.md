# User Documentation for CM Data Ingestion

## Overview of Functions

This document provides an overview and usage examples of the main ingest functions available in CM Data Ingestion. Each function is briefly described, followed by an example of how to call it with comments and code.

---

## Configuration

### General dlthub configuration

Firstly you need to setup some credentials and connection details for your destinations. You can start here https://dlthub.com/docs/general-usage/credentials/setup.

Here are some common settings you can setup:

```toml
# .dlt/config.toml

[normalize.data_writer]
disable_compression=true

[extract]
workers=1

[normalize]
workers=1

[load]
workers=1

[source]
max_table_nesting=1

[pipeline]
write_disposition="replace"

```toml
#.dlt/secrets.toml

[destination.postgres.credentials]
database = "postgres"
username = "postgres"
password = "postgres"
host = "localhost"
port = 5432

[destination.duckdb.credentials]
database = "./data/test.duckdb"

[destination.filesystem]
bucket_url = "s3://<your bucket name>"

[destination.filesystem.credentials]
region_name="eu-west-1"
profile_name="default"

[destination.motherduck.credentials]
database = "<your database>"
password = "<your password>"

```

### General structure of ingestion configuration

Each ingest function accepts two parameters - destination and source config. Destination is standard dlt destination (string or object) and source config is cm-data-ingestion specific.

General function calling pattern:
```python
ingest_xxxx(<destination>, <source config>)
```

General configuration pattern:
```python
config = {
    "items": [
        # items to download {}, different for each source
    ],
    "options": {
        # global options applied for each item
    }
}
```

This configuration allows you to explicitly define which data should be extracted, along with shared options applied across all items.


## Ingest Functions

### ingest_geoboundaries

Function for ingesting geoboundaries data. Used to load and process geographic boundary data.

#### Example Usage

```python
# Import the module
from cm_data_ingestion.pipelines.pipeline import ingest_geoboundaries
import dlt

# Define configuration
config = {
    "items": [
        {"admin_level": "ADM0"},
        {"admin_level": "ADM1"},
        {"admin_level": "ADM2"},
        {"admin_level": "ADM3"},
        {"admin_level": "ADM4"}
    ],
    "options": {
        "country_codes": ["cz"]
    }
}

# Ingest data into DuckDB
ingest_geoboundaries('duckdb', config)

# Ingest data into local filesystem
ingest_geoboundaries('filesystem', config)

# Ingest data into PostgreSQL database
ingest_geoboundaries('postgres', config)

# Ingest data into MotherDuck cloud database
ingest_geoboundaries('motherduck', config)
```

---

### ingest_gtfs

Function for ingesting GTFS data (General Transit Feed Specification). Used for processing public transit data.

#### Example Usage

```python
# Import the module
from cm_data_ingestion.pipelines.pipeline import ingest_gtfs
import dlt

# Define configuration
config = {
    "items": [
        {
            "country_code": "cz",
            "city": "Brno",
            "gtfs_type": "schedule",
            "provider": "Integrated Transit System of the South Moravian Region (IDS JMK)"
        }
    ],
    "options": {}
}

# Ingest data into DuckDB
ingest_gtfs('duckdb', config)

# Ingest data into local filesystem
ingest_gtfs('filesystem', config)

# Ingest data into PostgreSQL database
ingest_gtfs('postgres', config)

# Ingest data into MotherDuck cloud database
ingest_gtfs('motherduck', config)
```

---

### ingest_osm

Function for ingesting OpenStreetMap data. Used to load and process data from OpenStreetMap.

#### Example Usage

```python
# Import the module
from cm_data_ingestion.pipelines.pipeline import ingest_osm
import dlt

# Define configuration
config = {
    "items": [
        {"theme": "amenity"},
        {"theme": "aerialway"},
        {"theme": "aeroway"},
        {"theme": "barrier"},
        {"theme": "boundary"},
        {"theme": "building"},
        {"theme": "craft"},
        {"theme": "emergency"},
        {"theme": "geological"},
        {"theme": "highway"},
        {"theme": "historic"},
        {"theme": "landuse"},
        {"theme": "leisure"},
        {"theme": "man_made"},
        {"theme": "military"},
        {"theme": "natural"},
        {"theme": "office"},
        {"theme": "place"},
        {"theme": "power"},
        {"theme": "public_transport"},
        {"theme": "railway"},
        {"theme": "route"},
        {"theme": "shop"},
        {"theme": "telecom"},
        {"theme": "tourism"},
        {"theme": "waterway"}
    ],
    "options": {
        "country_codes": ["ad"]
    }
}

# Ingest data into DuckDB
ingest_osm('duckdb', config)

# Ingest data into local filesystem
ingest_osm('filesystem', config)

# Ingest data into PostgreSQL database
ingest_osm('postgres', config)

# Ingest data into MotherDuck cloud database
ingest_osm('motherduck', config)
```

---

### ingest_ovm

Function for ingesting Overture Maps data. Used for processing data from Overture Maps.

#### Example Usage

```python
# Import the module
from cm_data_ingestion.pipelines.pipeline import ingest_ovm
import dlt

# Define configuration
config = {
    "items": [
        {"theme": "places", "type": "place"},
        {"theme": "buildings", "type": "building"},
        {"theme": "addresses", "type": "address"},
        {"theme": "transportation", "type": "segment"},
        {"theme": "divisions", "type": "division"},
        {"theme": "divisions", "type": "division_area"},
        {"theme": "base", "type": "land_use"}
    ],
    "options": {
        "release": "2025-10-22.0",
        "bbox": [16.348858,49.087696,16.398211,49.112366]
    }
}


# Ingest data into DuckDB
ingest_ovm('duckdb', config)

# Ingest data into PostgreSQL database
ingest_ovm('postgres', config)

# Ingest data into local filesystem
ingest_ovm('filesystem', config)

# Ingest data into MotherDuck cloud database
ingest_ovm('motherduck', config)
```

---

### ingest_worldpop

Function for ingesting WorldPop data. Used to load and process population data.

#### Example Usage

```python
# Import the module
from cm_data_ingestion.pipelines.pipeline import ingest_worldpop
import dlt

# Define configuration
config = {
    "provider": "worldpop",
    "items": [
        {
            "theme": "population"
        }
    ],
    "options": {
        "country_codes": ["ad"]
    }
}

# Ingest data into DuckDB
ingest_worldpop('duckdb', config)

# Ingest data into PostgreSQL database
ingest_worldpop('postgres', config)

# Ingest data into local filesystem
ingest_worldpop('filesystem', config)

# Ingest data into MotherDuck cloud database
ingest_worldpop('motherduck', config)
```

---

## Staging dbt models

You can optionally use prepared dbt staging models in your downstream `dbt project`, which is standard way of implementation for further transformation logic. Each source has its own separate dbt project with staging models which you can easily import using `dpt deps` command.

Just add these to your `packages.yml` or `dependencies.yml` file.

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

After that you call the models using standard `dbt ref()` function in your dbt project models, e.g. `select * from ref('gtfs', 'stops')`.