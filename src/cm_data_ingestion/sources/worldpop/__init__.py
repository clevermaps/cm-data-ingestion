import dlt

from .helpers import raster_to_points

@dlt.source(name="worldpop")
def source(configs, temp_dir):
    
    for cfg in configs:
        print(cfg)
        table_name = cfg['table_name']
    
        yield dlt.resource(
            raster_to_points(cfg["url"], cfg['file_name'], temp_dir),
            name=f'{table_name}'
        )