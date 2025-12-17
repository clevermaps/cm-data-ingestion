from cm_data_ingestion.pipelines.pipeline import ingest_ovm
import dlt

# config = {
#         "items": [
#             {"theme": "places", "type": "place"},
#             {"theme": "buildings", "type": "building"},
#             {"theme": "addresses", "type": "address"},
#             {"theme": "transportation", "type": "segment"},
#             {"theme": "divisions", "type": "division"},
#             {"theme": "divisions", "type": "division_area"},
#             {"theme": "base", "type": "land_use"}
#         ],
#         "options": {
#             "release": "2025-10-22.0",
#             "bbox": [16.348858,49.087696,16.398211,49.112366]
#         }
#     }

config = {
    "items": [
        {"theme": "divisions", "type": "division_area"}
    ],
    "options": {
        "release": "2025-10-22.0",
        "bbox": [12.084961,48.458352,19.028320,51.179343]
    }
}

destination=dlt.destinations.duckdb("../data/data.duckdb")
#ingest_ovm(destination, config)

ingest_ovm('postgres', config)