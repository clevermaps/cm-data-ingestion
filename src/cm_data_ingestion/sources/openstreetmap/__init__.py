import dlt
from .helpers import get_data


# TODO logging

@dlt.resource(max_table_nesting=0)
def osm_resource(country_code, tag, value, release):
    yield from get_data(country_code, tag, value, release)