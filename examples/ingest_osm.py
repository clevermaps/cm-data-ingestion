from cm_data_ingestion.pipelines.pipeline import ingest_osm
import dlt

# config = {
#     "items": [
#         {"theme": "amenity"},
#         {"theme": "aerialway"},
#         {"theme": "aeroway"},
#         {"theme": "barrier"},
#         {"theme": "boundary"},
#         {"theme": "building"},
#         {"theme": "craft"},
#         {"theme": "emergency"},
#         {"theme": "geological"},
#         {"theme": "highway"},
#         {"theme": "historic"},
#         {"theme": "landuse"},
#         {"theme": "leisure"},
#         {"theme": "man_made"},
#         {"theme": "military"},
#         {"theme": "natural"},
#         {"theme": "office"},
#         {"theme": "place"},
#         {"theme": "power"},
#         {"theme": "public_transport"},
#         {"theme": "railway"},
#         {"theme": "route"},
#         {"theme": "shop"},
#         {"theme": "telecom"},
#         {"theme": "tourism"},
#         {"theme": "waterway"}
#     ],
#     "options": {
#         "country_codes": ["ad"]
#     }
# }

config = {
    "items": [
        {"theme": "boundary", "type": "administrative"}
    ],
    "options": {
        "country_codes": ["ad"]
    }
}

destination=dlt.destinations.duckdb("../data/data.duckdb")
#ingest_osm(destination, config)

#ingest_osm('filesystem', config)

ingest_osm('postgres', config)