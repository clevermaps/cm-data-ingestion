from cm_data_ingestion.pipelines.pipeline import ingest_gtfs
import dlt

config = {
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

destination=dlt.destinations.duckdb("../data/data.duckdb")
ingest_gtfs(destination, config)

ingest_gtfs('filesystem', config)

ingest_gtfs('postgres', config)