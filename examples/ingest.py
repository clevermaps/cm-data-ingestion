import logging

from cm_data_ingestion.pipelines import pipeline

logging.basicConfig(level=logging.INFO)

duckdb_path = "../data/data.duckdb"


## GTFS

config = {
    "provider": "gtfs",
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

pipeline.ingest_file('./data', 'parquet', config)

pipeline.ingest_file('s3://your-bucket-path/', 'csv', config)

pipeline.ingest_duckdb(duckdb_path, config, True)

#md_con_str = ''
#pipeline.ingest_motherduck(md_con_str, config, True)


## OVM

config = {
    "provider": "overturemaps",
    "items": [
        {"theme": "places", "type": "place", "table_name": "places__place"},
        {"theme": "buildings", "type": "building", "table_name": "buildings__building"},
        {"theme": "addresses", "type": "address", "table_name": "addresses__address"},
        {"theme": "transportation", "type": "segment", "table_name": "transportation__segment"},
        {"theme": "transportation", "type": "connector", "table_name": "transportation__connector"},
        {"theme": "divisions", "type": "division", "table_name": "divisions__division"},
        {"theme": "divisions", "type": "division_area", "table_name": "divisions__area"},
        {"theme": "base", "type": "land_use", "table_name": "base__land_use"}
    ],
    "options": {
        "release": "2025-09-24.0",
        "bbox": [15.54242618063797, 48.61653930468355, 17.646931589819502, 49.63325475249341]
    }
}

#pipeline.ingest_duckdb(duckdb_path, config, True)


## WorldPop

config = {
    "provider": "worldpop",
    "items": [
        {
            "theme": "population"
        }
    ],
    "options": {
        "country_codes": ["cz"]
    }
}

#pipeline.ingest_duckdb(duckdb_path, config, True)


## Geoboundaries

config = {
    "provider": "geoboundaries",
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

#pipeline.ingest_duckdb(duckdb_path, config, True)


## OpenStreetMap

config = {
    "provider": "openstreetmap",
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
        "country_codes": ["cz"]
    }
}

#pipeline.ingest_file('./data', 'jsonl', config)
pipeline.ingest_duckdb(duckdb_path, config, False)