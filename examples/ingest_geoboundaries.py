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

destination=dlt.destinations.duckdb("../data/data.duckdb")
#ingest_geoboundaries(destination, config)

#ingest_geoboundaries('filesystem', config)

ingest_geoboundaries('postgres', config)