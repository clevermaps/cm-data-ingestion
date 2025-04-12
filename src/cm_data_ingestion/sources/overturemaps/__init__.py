import dlt

from .helpers import get_data_bbox_arrow, get_data_bbox_divide_arrow

# Overturemaps schema
# https://til.simonwillison.net/overture-maps/overture-maps-parquet

# TODO logging

@dlt.resource(max_table_nesting=0)
def ovm_resource(theme, type, bbox, release, bbox_divide_zoom=None):

    if not bbox_divide_zoom:
        yield from get_data_bbox_arrow(theme, type, bbox, release)
    else:
        yield from get_data_bbox_divide_arrow(theme, type, bbox, release, bbox_divide_zoom)