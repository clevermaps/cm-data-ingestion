import pyarrow.dataset as ds
import pyarrow.fs
import duckdb
import mercantile

from .settings import OVM_S3_URL_TEMPLATE_DUCKDB, OVM_S3_URL_TEMPLATE_ARROW

import logging

logger = logging.getLogger(__name__)

def get_duckdb_con():
    """
    Create and configure a DuckDB connection with HTTPFS and spatial extensions.

    Returns:
        duckdb.DuckDBPyConnection: Configured DuckDB connection.
    """
    con = duckdb.connect(
        config={
            'threads': 1,
            'max_memory': '6GB',
        }
    )

    con.execute('install httpfs')
    con.execute('install spatial')
    con.execute('load httpfs')
    con.execute('load spatial')

    con.execute("set s3_region='us-west-2'")
    con.execute("SET allow_persistent_secrets=false")

    return con

# slow
def get_data_bbox_duckdb(theme, type, xmin, ymin, xmax, ymax, release):
    """
    Query Overturemaps data from DuckDB parquet files filtered by bounding box.

    Args:
        theme (str): Theme of the data.
        type (str): Type of the data.
        xmin (float): Minimum x coordinate of bounding box.
        ymin (float): Minimum y coordinate of bounding box.
        xmax (float): Maximum x coordinate of bounding box.
        ymax (float): Maximum y coordinate of bounding box.
        release (str): Release version.

    Yields:
        list: List of records in batches.
    """
    url = OVM_S3_URL_TEMPLATE_DUCKDB.format(release=release, theme=theme, type=type)

    logger.info(f"Querying DuckDB parquet files from URL: {url}")

    con = get_duckdb_con()

    sql = f"""
        SELECT 
            *
        FROM read_parquet('{url}', filename=true, hive_partitioning=1)
        WHERE bbox.xmin > {xmin}
        AND bbox.ymin > {ymin}
        AND bbox.xmax < {xmax}
        AND bbox.ymax < {ymax}
    """

    record_batch_reader = con.execute(sql).fetch_record_batch()

    while True:
        try:
            chunk = record_batch_reader.read_next_batch()
            logger.debug(f"Yielding batch with {len(chunk.to_pylist())} records")
            yield chunk.to_pylist()
        except StopIteration:
            break

    con.close()


def get_data_bbox_divide_arrow(theme, type, bbox, release, divide_zoom):
    """
    Divide bounding box into tiles and yield data for each tile using Arrow format.

    Args:
        theme (str): Theme of the data.
        type (str): Type of the data.
        bbox (tuple): Bounding box coordinates (xmin, ymin, xmax, ymax).
        release (str): Release version.
        divide_zoom (int): Zoom level for dividing bounding box.

    Yields:
        Iterator: Data chunks for each tile.
    """
    bboxes = divide_bbox((bbox), divide_zoom)

    logger.info(f"Dividing bounding box into tiles at zoom level {divide_zoom}, total tiles: {len(bboxes)}")

    for bbox in bboxes:
        logger.debug(f"Processing tile bounding box: {bbox}")
        yield from get_data_bbox_arrow(theme, type, bbox, release)


def get_data_bbox_arrow(theme, type, bbox, release):
    """
    Query Overturemaps data using Arrow format filtered by bounding box.

    Args:
        theme (str): Theme of the data.
        type (str): Type of the data.
        bbox (tuple): Bounding box coordinates (xmin, ymin, xmax, ymax).
        release (str): Release version.

    Yields:
        pyarrow.RecordBatch: Record batches matching the filter.
    """
    url = OVM_S3_URL_TEMPLATE_ARROW.format(release=release, theme=theme, type=type)

    logger.info(url)

    s3 = pyarrow.fs.S3FileSystem(region='us-west-2')

    dataset = ds.dataset(url, filesystem=s3, format="parquet")

    xmin, ymin, xmax, ymax = bbox[0], bbox[1], bbox[2], bbox[3]

    filter_expr = (
        (ds.field("bbox", "xmin") > xmin) &
        (ds.field("bbox", "ymin") > ymin) &
        (ds.field("bbox", "xmax") < xmax) &
        (ds.field("bbox", "ymax") < ymax)
    )

    logger.debug(filter_expr)

    # TODO columns parametric
    scanner = ds.Scanner.from_dataset(
        dataset,
        filter=filter_expr,
        batch_size=100000
    )

    for record_batch in scanner.to_batches():
        if record_batch.num_rows > 0:

            yield record_batch


def divide_bbox(bbox, zoom):
    """
    Divide a bounding box into tiles at a given zoom level.

    Args:
        bbox (tuple): Bounding box coordinates (xmin, ymin, xmax, ymax).
        zoom (int): Zoom level.

    Returns:
        list: List of bounding boxes for each tile.
    """
    tiles = list(mercantile.tiles(*bbox, zoom))

    bboxes = []
    for tile in tiles:
        quadkey = mercantile.quadkey(tile)
        bounds = mercantile.bounds(tile)

        bboxes.append(bounds)

    return bboxes
