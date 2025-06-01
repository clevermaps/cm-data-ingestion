import dlt
import os

from cm_data_ingestion.sources.openstreetmap import osm_resource

# Excract data from OSM to Duckdb
pipeline = dlt.pipeline(
    pipeline_name="osm",
    destination=dlt.destinations.duckdb("{}/openstreetmap.db".format(os.getcwd())),
    dataset_name='osm'
)

result = pipeline.run(
    osm_resource(
            country_code='ad',
            tag='place',
            value='town',
            release='2025-02-19.0',
            element_type='node'  # Example: filter for nodes only
            # filter='categories.'
        ),
        table_name='places__place'
    )
print(result)