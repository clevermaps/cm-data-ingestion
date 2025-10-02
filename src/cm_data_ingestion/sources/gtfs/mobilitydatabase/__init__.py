import dlt
from .helpers import get_data

@dlt.source(name="gtfs_mobility")
def gtfs_mobility(configs):
    """
    configs: list[dict], kde každý dict má např. theme, type, bbox, release
    """
    for cfg in configs:
        print(cfg)

        country_code = cfg.get('country_code')
        gtfs_type = cfg.get('gtfs_type')
        city = cfg.get('city')

        table_name = f'{gtfs_type}__{country_code}' if not city else f'{gtfs_type}__{country_code}_{city}'
    
        # TODO max_table_nesting=0 to pipeline config
        yield dlt.resource(
            get_data(
                country_code, 
                city, 
                gtfs_type, 
                cfg.get('provider'), 
                cfg.get('x-coordinate'),
                cfg.get('y-coordinate')
            ),
            name=table_name,
            max_table_nesting=0
        )