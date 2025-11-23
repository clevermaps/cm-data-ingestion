import dlt

from .helpers import get_data_bbox_arrow

# Overturemaps schema
# https://til.simonwillison.net/overture-maps/overture-maps-parquet


@dlt.source(name="overturemaps")
def ovm(items, options):
    """
    DLT source for Overturemaps data ingestion.

    Args:
        items (list): List of dictionaries, each containing 'theme', 'type', and 'table_name'.
        options (dict): Options dictionary, expected to contain 'bbox' and 'release'.

    Yields:
        dlt.resource: Data resource for each item.
    """
    for item in items:
        print(item)
        ovm_theme = item["theme"]
        ovm_type = item["type"]
        table_name = item['table_name']

        # TODO max_table_nesting=0 to pipeline config
        yield dlt.resource(
            get_data_bbox_arrow(ovm_theme, ovm_type, options['bbox'], options['release']),
            name=f'{table_name}',
            max_table_nesting=0
        )
