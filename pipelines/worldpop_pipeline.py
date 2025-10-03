import dlt
from cm_data_ingestion.sources.worldpop import worldpop
from common.runners import run_dbt, run_dlt
from common.config import load_source_config

temp_dir = './data'


destination = dlt.destinations.duckdb("./data/dlt.duckdb")
config = load_source_config("./configs/worldpop_config.json")['data']
dlt_resource = worldpop(config, temp_dir)
run_dlt(dlt_resource, destination, 'worldpop_raw')

run_dbt(destination, 'worldpop_dbt', 'dbt/worldpop')