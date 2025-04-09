import dlt
import os
from cm_data_ingestion.sources.overturemaps import ovm_resource

# http://bboxfinder.com/#0.000000,0.000000,0.000000,0.000000

# ivancice
bbox = (16.297102, 49.076227, 16.454601, 49.128600)
# cz
#bbox = (12.030029,48.487486,18.907471,51.055207)

release = '2025-02-19.0'

# Excract data from OvertureMaps to Duckdb
#pipeline = dlt.pipeline(destination=dlt.destinations.duckdb("{}/overturemaps.db".format(os.getcwd())), dataset_name='ovm')

# Excract data from OvertureMaps to local file
dlt.config["destination.filesystem.bucket_url"] = "file://{}".format(os.getcwd())
pipeline = dlt.pipeline(destination="filesystem", dataset_name="ovm")

result = pipeline.run(
    ovm_resource(
            theme='places', 
            type='place', 
            bbox=bbox, 
            release=release
        ),
        table_name='places__place'
    )
print(result)

# result = pipeline.run(
#     ovm_resource(
#             theme='addresses', 
#             type='address', 
#             bbox=bbox, 
#             release=release
#         ),
#         table_name='addresses__address'
#     )
# print(result)

# result = pipeline.run(
#     ovm_resource(
#             theme='buildings', 
#             type='buildings', 
#             bbox=bbox, 
#             release=release
#         ),
#         table_name='buildings__building'
#     )
# print(result)

# result = pipeline.run(
#     ovm_resource(
#             theme='transportation', 
#             type='segment', 
#             bbox=bbox, 
#             release=release
#         ),
#         table_name='transportation__segment'
#     )
# print(result)


# result = pipeline.run(
#     ovm_resource(
#             theme='transportation', 
#             type='connector', 
#             bbox=bbox, 
#             release=release
#         ),
#         table_name='transportation__connector'
#     )
# print(result)


# result = pipeline.run(
#     ovm_resource(
#             theme='divisions', 
#             type='division', 
#             bbox=bbox, 
#             release=release
#         ),
#         table_name='divisions__division'
#     )
# print(result)


# result = pipeline.run(
#     ovm_resource(
#             theme='base', 
#             type='land_use', 
#             bbox=bbox, 
#             release=release
#         ),
#         table_name='base__land_use'
#     )
# print(result)