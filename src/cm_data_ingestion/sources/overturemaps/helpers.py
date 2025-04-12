import pyarrow.dataset as ds
import pyarrow.fs
import duckdb
import mercantile

from .settings import OVM_S3_URL_TEMPLATE_DUCKDB, OVM_S3_URL_TEMPLATE_ARROW


def get_duckdb_con():

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

    url = OVM_S3_URL_TEMPLATE_DUCKDB.format(release=release, theme=theme, type=type)

    print(url)

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
            yield chunk.to_pylist()
        except StopIteration:
            break

    con.close()


def get_data_bbox_divide_arrow(theme, type, bbox, release, divide_zoom):

    bboxes = divide_bbox((bbox), divide_zoom)
    print(len(bboxes))

    for bbox in bboxes:
        print(bbox)
        yield from get_data_bbox_arrow(theme, type, bbox, release)


def get_data_bbox_arrow(theme, type, bbox, release):

    url = OVM_S3_URL_TEMPLATE_ARROW.format(release=release, theme=theme, type=type)

    print(url)

    s3 = pyarrow.fs.S3FileSystem(region='us-west-2')

    dataset = ds.dataset(url, filesystem=s3, format="parquet")

    xmin, ymin, xmax, ymax = bbox[0], bbox[1], bbox[2], bbox[3]

    filter_expr = (
        (ds.field("bbox", "xmin") > xmin) &
        (ds.field("bbox", "ymin") > ymin) &
        (ds.field("bbox", "xmax") < xmax) &
        (ds.field("bbox", "ymax") < ymax)
    )

    print(filter_expr)

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

    tiles = list(mercantile.tiles(*bbox, zoom))

    bboxes = []
    for tile in tiles:
        quadkey = mercantile.quadkey(tile)
        bounds = mercantile.bounds(tile)

        bboxes.append(bounds)

    return bboxes
        