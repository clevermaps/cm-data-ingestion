import dlt
from cm_data_ingestion.sources.worldpop import worldpop

from common.config import load_source_config

temp_dir = './data'

config = load_source_config("./configs/worldpop_config.json")['data']

pipeline = dlt.pipeline(
    pipeline_name="worldpop_pipeline",
    destination=dlt.destinations.duckdb('./data/dlt.duckdb'),
    dataset_name="worldpop"
)

result = pipeline.run(worldpop(config, temp_dir))
print(result)
