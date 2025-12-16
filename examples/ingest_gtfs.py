from cm_data_ingestion.pipelines.pipeline import ingest_duckdb

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

ingest_duckdb('./data.duckdb', config)