import dlt
import logging
from .helpers import get_data

logger = logging.getLogger(__name__)


@dlt.source(name="openstreetmap")
def source(items, temp_dir):
    """
    DLT source for OpenStreetMap data ingestion.

    Args:
        items (list): List of dictionaries, each containing 'country_codes', 'tag', 'value', and optional parameters.
        'temp_dir'.

    Yields:
        dlt.resource: Data resource for each country code in items.
    """
    logger.info(f"Starting openstreetmap source with {len(items)} items")

    for item in items:
        logger.info(f"Processing item: {item}")
        tag = item["tag"]
        value = item["value"]
        table_name = item['table_name']

        yield dlt.resource(
            get_data(
                temp_dir,
                item['country_code'],
                tag,
                value,
                item.get("element_type", None),
                item.get("target_date_range", None),
                item.get("target_date_tolerance_days", 0),
                item.get("prefer_older", False)
            ),
            name=table_name,
            #max_table_nesting=0
        )
