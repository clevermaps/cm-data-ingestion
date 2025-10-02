import dlt
from cm_data_ingestion.sources.openstreetmap import osm
from common.config import load_source_config

osm_cfg = load_source_config("./configs/osm_config.json")['downloads']
temp_dir = './data/'

pipeline = dlt.pipeline(
    pipeline_name='osm_pipeline',
    destination=dlt.destinations.duckdb('./data/dlt.duckdb'),
    dataset_name='osm'
)

result = pipeline.run(osm(osm_cfg, temp_dir))
print(result)