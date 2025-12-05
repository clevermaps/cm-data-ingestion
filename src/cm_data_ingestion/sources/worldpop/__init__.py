import dlt
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from .helpers import raster_to_points

@dlt.source(name="worldpop")
def source(items, temp_dir):
    
    for item in items:
        logger.info(f"Processing item: {item}")
        table_name = item['table_name']
    
        yield dlt.resource(
            raster_to_points(item["url"], item['file_name'], temp_dir),
            name=f'{table_name}'
        )