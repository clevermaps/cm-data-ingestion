import dlt
from common.config import load_source_config
from common.runners import run_dbt, run_dlt

from cm_data_ingestion.sources.gtfs.mobilitydatabase import gtfs_mobility


destination = dlt.destinations.duckdb("./data/dlt.duckdb")
config = load_source_config("./configs/gtfs_mobility_ol_config.json")['downloads']
dlt_resource = gtfs_mobility(config)
run_dlt(dlt_resource, destination, 'gtfs_raw')

run_dbt(destination, 'gtfs_dbt', 'dbt/gtfs')