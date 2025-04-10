import pyarrow.dataset as ds
import pyarrow.fs
import duckdb

from .settings import OVM_S3_URL_TEMPLATE


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

    return con


def get_data_bbox_duckdb(theme, type, xmin, ymin, xmax, ymax, release, filter):

    url = OVM_S3_URL_TEMPLATE.format(release=release, theme=theme, type=type)

    print(url)



    con = get_duckdb_con()

    sql = f"""
        SELECT 
            * replace (st_astext(geometry) as geometry)
        FROM read_parquet('{url}', filename=true, hive_partitioning=1)
        WHERE bbox.xmin > {xmin}
        AND bbox.ymin > {ymin}
        AND bbox.xmax < {xmax}
        AND bbox.ymax < {ymax}
    """

    if filter:
        sql = sql + ' AND ( {} )'.format(filter)

    print(sql)

    record_batch_reader = con.execute(sql).fetch_record_batch()

    while True:
        try:
            chunk = record_batch_reader.read_next_batch()
            yield chunk.to_pylist()
        except StopIteration:
            break

    con.close()


def get_data_bbox_arrow(theme, type, xmin, ymin, xmax, ymax, release):

    url = OVM_S3_URL_TEMPLATE.format(release=release, theme=theme, type=type)

    print(url)

    s3 = pyarrow.fs.S3FileSystem(region='us-west-2')

    dataset = ds.dataset(url, filesystem=s3, format="parquet")

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
        if record_batch:
            
            yield record_batch

        # df_batch = record_batch.to_pandas()

        # if not df_batch.empty:
        #     yield df_batch