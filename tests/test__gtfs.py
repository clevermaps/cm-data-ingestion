from utils import ALL_DESTINATIONS, assert_load_info, load_table_counts
import pytest
import dlt
from cm_data_ingestion.pipelines.pipeline import ingest_gtfs

ALL_DESTINATIONS = ['duckdb', 'filesystem', 'motherduck']

@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    
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

    load_info = ingest_gtfs(destination_name, config)
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