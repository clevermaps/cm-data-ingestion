import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import dlt
from cm_data_ingestion.sources.worldpop.helpers import make_raster_resource

print("sys.argv:", sys.argv)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("[ERROR] Usage: python pipelines/worldpop_pipeline.py <path_to_json>")
        sys.exit(1)

    json_path = sys.argv[1]

    pipeline = dlt.pipeline(
        pipeline_name="raster_data",
        destination="duckdb",
        dataset_name="data",
        full_refresh=True
    )

    resource = make_raster_resource(json_path)
    print("Writing data to DuckDB...")
    pipeline.run(resource())
    print("✅ Done.")
