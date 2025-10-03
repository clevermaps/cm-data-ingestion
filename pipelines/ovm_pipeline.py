import dlt
from cm_data_ingestion.sources.overturemaps import ovm
from common.config import load_source_config
from common.runners import run_dbt, run_dlt


destination = dlt.destinations.duckdb("./data/dlt.duckdb")
config = load_source_config("./configs/ovm_config.json")
dlt_resource = ovm(config['data'], config['bbox'], config['release'])
run_dlt(dlt_resource, destination, 'ovm_raw')


run_dbt(destination, 'ovm_dbt', 'dbt/ovm')