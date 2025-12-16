from cm_data_ingestion.pipelines.pipeline import ingest_duckdb

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

ingest_duckdb('./data.duckdb', config)