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

destination=dlt.destinations.duckdb("../data/data.duckdb")
ingest_worldpop(destination, config)

ingest_worldpop('postgres', config)

ingest_worldpop('filesystem', config)