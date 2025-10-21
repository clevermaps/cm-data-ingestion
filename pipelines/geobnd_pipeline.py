import dlt
from common.config import load_source_config
from common.runners import run_dbt, run_dlt

from cm_data_ingestion.sources.geoboundaries import geoboundaries


destination = dlt.destinations.duckdb("./data/dlt.duckdb")
config = load_source_config("./configs/geobnd_config.json")
dlt_resource = geoboundaries(config)
run_dlt(dlt_resource, destination, 'geobnd_raw')

run_dbt(destination, 'geobnd_dbt', 'dbt/geobnd')