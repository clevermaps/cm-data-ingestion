import dlt
import logging

logger = logging.getLogger(__name__)

from .helpers import raster_to_points

@dlt.source(name="worldpop")
def source(items, temp_dir):
    
    for item in items:
        logger.info(f"Processing item: {item}")
        table_name = item['table_name']
    
        yield dlt.resource(
            raster_to_points(item["country"], item['theme'], temp_dir),
            name=f'{table_name}'
        )