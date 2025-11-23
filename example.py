import logging

from cm_data_ingestion.pipelines import pipeline

logging.basicConfig(level=logging.INFO)

duckdb_path = "./data/data.duckdb"


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

dbt_params = [
    {
        "name": "stops",
        "alias": "stops"
    }
]

#pipeline.ingest_file('./data', 'parquet', config)

#pipeline.ingest_file('s3://clevermaps-data-lake/temp/dlt/', 'csv', config)

#pipeline.ingest_duckdb(duckdb_path, config, True, dbt_params)

#md_con_str = ''
#pipeline.ingest_motherduck(md_con_str, config, True)


## OVM

config = {
    "provider": "overturemaps",
    "items": [
        {"theme": "places", "type": "place", "table_name": "places__place"},
        # {"theme": "buildings", "type": "building", "table_name": "buildings__building"},
        # {"theme": "addresses", "type": "address", "table_name": "addresses__address"},
        # {"theme": "transportation", "type": "segment", "table_name": "transportation__segment"},
        # {"theme": "transportation", "type": "connector", "table_name": "transportation__connector"},
        # {"theme": "divisions", "type": "division", "table_name": "divisions__division"},
        # {"theme": "divisions", "type": "division_area", "table_name": "divisions__area"},
        # {"theme": "base", "type": "land_use", "table_name": "base__land_use"}
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
            "theme": "population", 
            "country": "cze"
        }
    ]
}

#pipeline.ingest_duckdb(duckdb_path, config, True)


## Geoboundaries

config = {
    "provider": "geoboundaries",
    "items": [
        {
            "country_code": "CZE", 
            "admin_level": "ADM0"
        },
        {
            "country_code": "CZE", 
            "admin_level": "ADM1"
        },
        {
            "country_code": "CZE", 
            "admin_level": "ADM2"
        },
        {
            "country_code": "CZE", 
            "admin_level": "ADM3"
        },
        {
            "country_code": "CZE", 
            "admin_level": "ADM4"
        }
    ]
}

#pipeline.ingest_duckdb(duckdb_path, config, True)


## OpenStreetMap

config = {
    "provider": "openstreetmap",
    "items": [
        {
            "theme": "amenity", 
            "country_code": "cz"
        }
    ]
}

#pipeline.ingest_file('./data', 'jsonl', config)
pipeline.ingest_duckdb(duckdb_path, config, True)