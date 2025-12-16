from cm_data_ingestion.pipelines.pipeline import ingest_duckdb

config = {
        "provider": "overturemaps",
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

ingest_duckdb('./data.duckdb', config)