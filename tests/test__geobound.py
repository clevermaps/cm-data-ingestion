from utils import ALL_DESTINATIONS, assert_load_info, load_table_counts
import pytest
import dlt
from cm_data_ingestion.pipelines.pipeline import _ingest_geoboundaries

ALL_DESTINATIONS = ['duckdb']

@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    
    config = {
        "provider": "geoboundaries",
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

    load_info = _ingest_geoboundaries(destination_name, config)
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