import dlt

from .helpers import get_data


@dlt.source(name="geoboundaries")
def geoboundaries(items):
    
    for item in items:
        yield dlt.resource(
            get_data(item['country_code'], item['admin_level']),
            name=item['table_name'],
            max_table_nesting=0
        )

