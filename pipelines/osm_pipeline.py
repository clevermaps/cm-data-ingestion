import dlt
import os

from cm_data_ingestion.sources.openstreetmap import osm_resource

# http://bboxfinder.com/#0.000000,0.000000,0.000000,0.000000

# ivancice
bbox = (16.297102, 49.076227, 16.454601, 49.128600)
# cz
# bbox = (12.030029,48.487486,18.907471,51.055207)

release = '2025-02-19.0'

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
            release=release,
            # filter='categories.'
        ),
        table_name='places__place'
    )
print(result)