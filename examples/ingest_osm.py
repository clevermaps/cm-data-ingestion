from cm_data_ingestion.pipelines.pipeline import ingest_duckdb

config = {
    "provider": "openstreetmap",
    "items": [
        {"theme": "boundary", "type": "administrative"},
    ],
    "options": {
        "country_codes": ["cz"]
    }
}

load_info = ingest_duckdb('./data.duckdb', config)
print(load_info)