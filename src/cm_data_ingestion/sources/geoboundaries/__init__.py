import dlt

from .helpers import get_data


@dlt.source(name="geoboundaries")
def geoboundaries(configs):
    
    for cfg in configs:
        yield dlt.resource(
            get_data(cfg['country_code'], cfg['admin_level']),
            name=cfg['table_name'],
            max_table_nesting=0
        )

