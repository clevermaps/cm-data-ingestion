import dlt

from .helpers import get_data_bbox_arrow

# Overturemaps schema
# https://til.simonwillison.net/overture-maps/overture-maps-parquet

@dlt.resource(max_table_nesting=0)
def ovm_resource(theme, type, bbox, release):

    xmin, ymin, xmax, ymax = bbox[0], bbox[1], bbox[2], bbox[3]
    data = get_data_bbox_arrow(theme, type, xmin, ymin, xmax, ymax, release)

    yield data