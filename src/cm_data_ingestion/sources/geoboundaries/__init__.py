import dlt

from .helpers import get_data


@dlt.source(name="geoboundaries")
def geoboundaries(country_code, admin_level, table_name):
    
    yield dlt.resource(
        get_data(country_code, admin_level),
        name=f'{table_name}',
        max_table_nesting=0
    )

