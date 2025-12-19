from cm_data_ingestion.pipelines.pipeline import ingest_worldpop
import dlt

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

# set credentials and connection details in .dlt/config.toml and .dlt/secrets.toml

ingest_worldpop('duckdb', config)

ingest_worldpop('postgres', config)

ingest_worldpop('filesystem', config)

ingest_worldpop('motherduck', config)