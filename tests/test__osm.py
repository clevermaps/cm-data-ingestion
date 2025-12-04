from utils import ALL_DESTINATIONS, assert_load_info, load_table_counts
import pytest
import dlt
from cm_data_ingestion.pipelines.pipeline import _ingest_osm

ALL_DESTINATIONS = ['duckdb']

@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    
    config = {
        "provider": "openstreetmap",
        "items": [
            {"theme": "amenity"},
            {"theme": "aerialway"},
            {"theme": "aeroway"},
            {"theme": "barrier"},
            {"theme": "boundary"},
            {"theme": "building"},
            {"theme": "craft"},
            {"theme": "emergency"},
            {"theme": "geological"},
            {"theme": "highway"},
            {"theme": "historic"},
            {"theme": "landuse"},
            {"theme": "leisure"},
            {"theme": "man_made"},
            {"theme": "military"},
            {"theme": "natural"},
            {"theme": "office"},
            {"theme": "place"},
            {"theme": "power"},
            {"theme": "public_transport"},
            {"theme": "railway"},
            {"theme": "route"},
            {"theme": "shop"},
            {"theme": "telecom"},
            {"theme": "tourism"},
            {"theme": "waterway"}
        ],
        "options": {
            "country_codes": ["ad"]
        }
    }

    load_info = _ingest_osm(destination_name, config)
    print(load_info)

    # check run result
    assert_load_info(load_info)

    # table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    # table_counts = load_table_counts(pipeline, *table_names)

    # print(table_counts)

    # # check if table exists
    # expected_tables = ["geoboundaries__adm0"]
    # assert set(table_counts.keys()) >= set(expected_tables)

    # # check row count greater than zero
    # assert table_counts["geoboundaries__adm0"] > 0