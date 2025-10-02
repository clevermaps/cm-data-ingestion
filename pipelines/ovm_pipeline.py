import dlt
import os
from cm_data_ingestion.sources.overturemaps import ovm
from common.config import load_source_config

# http://bboxfinder.com/#0.000000,0.000000,0.000000,0.000000

# ivancice
bbox = (16.297102, 49.076227, 16.454601, 49.128600)
# cz
#bbox = (12.030029,48.487486,18.907471,51.055207)

release = '2025-09-24.0'

ovm_cfg = load_source_config("./configs/ovm_config.json")

# Excract data from OvertureMaps to Duckdb
pipeline = dlt.pipeline(
    pipeline_name='ovm_pipeline',
    destination=dlt.destinations.duckdb("./data/dlt.duckdb"), 
    dataset_name='ovm'
)

# # Excract data from OvertureMaps to local file
# dlt.config["destination.filesystem.bucket_url"] = "file://{}".format(os.getcwd())
# pipeline = dlt.pipeline(destination="filesystem", dataset_name="ovm")

result = pipeline.run(ovm(ovm_cfg, bbox, release))
print(result)