import dlt
from common.config import load_source_config

from cm_data_ingestion.sources.gtfs.mobilitydatabase import gtfs_mobility
from cm_data_ingestion.sources.gtfs.transit import gtfs_transit


def run_gtfs_mobility():

    config = load_source_config("./configs/gtfs_mobility_config.json")['downloads']

    pipeline = dlt.pipeline(
        pipeline_name="gtfs_mobility_pipeline",
        destination=dlt.destinations.duckdb('./data/dlt.duckdb'),
        dataset_name='gtfs_mobility',
    )

    result = pipeline.run(gtfs_mobility(config), write_disposition="replace")
    print(result)


def run_gtfs_transit():

    config = load_source_config("./configs/gtfs_transit_config.json")['downloads']

    pipeline = dlt.pipeline(
        pipeline_name="gtfs_transit_pipeline",
        destination=dlt.destinations.duckdb('./data/dlt.duckdb'),
        dataset_name='gtfs_transit',
    )

    result = pipeline.run(gtfs_transit(config), write_disposition="replace")
    print(result)


run_gtfs_mobility()
#run_gtfs_transit()