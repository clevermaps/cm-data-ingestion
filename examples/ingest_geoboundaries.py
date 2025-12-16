from cm_data_ingestion.pipelines.pipeline import ingest_duckdb

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

ingest_duckdb('./data.duckdb', config)