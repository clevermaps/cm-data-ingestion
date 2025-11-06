import dlt
from .helpers import get_data

@dlt.source(name="gtfs_mobility")
def gtfs_mobility(items):
    """
    items: list[dict], kde každý dict má např. theme, type, bbox, release
    """
    for item in items:
        print(item)

        country_code = item.get('country_code')
        gtfs_type = item.get('gtfs_type')
        city = item.get('city')

        table_name = f'{gtfs_type}__{country_code}' if not city else f'{gtfs_type}__{country_code}_{city}'
    
        # TODO max_table_nesting=0 to pipeline config
        yield dlt.resource(
            get_data(
                country_code, 
                city, 
                gtfs_type, 
                item.get('provider'), 
                item.get('x-coordinate'),
                item.get('y-coordinate')
            ),
            name=table_name,
            max_table_nesting=0
        )