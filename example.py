from cm_data_ingestion.pipelines import pipeline


duckdb_path = "./data/dlt.duckdb"

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
        "alias": "stg_stops"
    }
]

#pipeline.ingest_duckdb(duckdb_path, config, True, dbt_params)

# md_con_str = 'md:///gtfs?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImthcmVsLnBzb3RhQGNsZXZlcm1hcHMuaW8iLCJzZXNzaW9uIjoia2FyZWwucHNvdGEuY2xldmVybWFwcy5pbyIsInBhdCI6IkdGQ1Z1QTFGejNUeFM1dmZ5ZVJhZTEtN1BLSVk0V3E0WnJLc3Z2V2pYdU0iLCJ1c2VySWQiOiIzYjMxMTc4Ni1mYmM1LTRlOTEtYTZmYS1mOTRhZDlkOTBjODYiLCJpc3MiOiJtZF9wYXQiLCJyZWFkT25seSI6ZmFsc2UsInRva2VuVHlwZSI6InJlYWRfd3JpdGUiLCJpYXQiOjE3NjIyNTc1OTd9.St-f0ClX3P-c_AEpynZ_BHvi_AVJXJE-KjE4jL_3Euo'
# ingest_motherduck(md_con_str, items)


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
        "bbox": [17.152061,49.525431,17.363892,49.638510]
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
            "admin_level": "ADM3"
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

pipeline.ingest_duckdb(duckdb_path, config, True)