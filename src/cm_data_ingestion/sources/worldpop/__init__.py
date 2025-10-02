import dlt

from .helpers import raster_to_points

@dlt.source(name="worldpop")
def worldpop(configs, temp_dir):
    """
    configs: list[dict], kde každý dict má např. theme, type, bbox, release
    """
    for cfg in configs:
        print(cfg)
        table_name = cfg['table_name']
    
        # TODO max_table_nesting=0 to pipeline config
        yield dlt.resource(
            raster_to_points(cfg["url"], cfg['file_name'], temp_dir),
            name=f'{table_name}',
            max_table_nesting=0
        )