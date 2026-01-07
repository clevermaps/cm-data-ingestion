from cm_data_ingestion.pipelines.pipeline import ingest_geoboundaries
import dlt

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

# set credentials and connection details in .dlt/config.toml and .dlt/secrets.toml

ingest_geoboundaries('duckdb', config)

ingest_geoboundaries('filesystem', config)

ingest_geoboundaries('postgres', config)

ingest_geoboundaries('motherduck', config)