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


def get_data_bbox(theme, type, xmin, ymin, xmax, ymax, release, filter):

    url = OVM_S3_URL_TEMPLATE.format(release=release, theme=theme, type=type)

    print(url)

    con = get_duckdb_con()

    sql = f"""
        SELECT * replace (st_astext(geometry) as geometry)
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


# TODO prilis narocne vypocetne, kombinace s bbox pripadne
# def get_data_admin(theme, type, country, admin_level, admin_name, release):

#     con = get_duckdb_con()

#     adm_url = f'https://github.com/wmgeolab/geoBoundaries/raw/9469f09/releaseData/gbOpen/{country}/{admin_level}/geoBoundaries-{country}-{admin_level}_simplified.geojson'

#     ovm_url = OVM_S3_URL_TEMPLATE.format(release=release, theme=theme, type=type)

#     con.sql(f"""
#         create table ovm AS
#         with tmp as
#         (   
#             select
#                 shapeName,
#                 geom
#             from st_read('{adm_url}')
#         )
#         select a.* replace (st_astext(a.geometry) as geometry)
#         from read_parquet('{ovm_url}', filename=true, hive_partitioning=1) a
#         left join tmp b
#         on st_intersects(a.geometry, b.geom)
#         and b.shapeName = '{admin_name}'
#     """)

#     data = con.sql(f'select * from ovm').df().to_json(orient='records')
#     data_dict = json.loads(data)

#     con.close()

#     return data_dict