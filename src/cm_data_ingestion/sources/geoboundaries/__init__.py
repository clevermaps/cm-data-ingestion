import dlt
import logging

from .helpers import get_data

logger = logging.getLogger(__name__)


@dlt.source(name="geoboundaries")
def source(items):
    """
    DLT source for geoboundaries data ingestion.

    Args:
        items (list): List of dictionaries, each containing 'country_code', 'admin_level', and 'table_name'.

    Yields:
        dlt.resource: Data resource for each item.
    """
    logger.info(f"Starting geoboundaries source with {len(items)} items")

    for item in items:
        logger.info(f"Processing item: {item}")
        yield dlt.resource(
            get_data(item['url']),
            name=item['table_name']
        )
