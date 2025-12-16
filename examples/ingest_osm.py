from cm_data_ingestion.pipelines.pipeline import ingest_duckdb

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
        "country_codes": ["ad"]
    }
}

ingest_duckdb('./data.duckdb', config)