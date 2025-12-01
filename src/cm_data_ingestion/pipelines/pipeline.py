from pathlib import Path
import os
import tempfile
import pycountry

import dlt

from .common.helpers import run_dbt, run_dlt, get_worldpop_url

from ..sources.gtfs.mobilitydatabase import gtfs_mobility
from ..sources.overturemaps import ovm
from ..sources.worldpop import worldpop
from ..sources.geoboundaries import geoboundaries
from ..sources.openstreetmap import osm


BASE_DIR = Path(__file__).parent


def _ingest_gtfs(destination, config, dbt_run, dbt_params):

    DBT_DIR = BASE_DIR / "dbt/gtfs"

    dlt_resource = gtfs_mobility(config['items'])
    run_dlt(dlt_resource, destination, 'gtfs_raw')

    if dbt_run:
        run_dbt(destination, 'gtfs_stg', str(DBT_DIR), dbt_params)


def _ingest_ovm(destination, config, dbt_run, dbt_params):

    DBT_DIR = BASE_DIR / "dbt/ovm"

    dlt_resource = ovm(config['items'], config['options'])
    run_dlt(dlt_resource, destination, 'ovm_raw')

    if dbt_run:
        if not dbt_params:
            dbt_params = [
                {
                    "name": '{}_{}'.format(item["theme"], item["type"]),
                    "alias": '{}_{}'.format(item["theme"], item["type"])
                }
                for item in config['items']
            ]
        run_dbt(destination, 'ovm_stg', str(DBT_DIR), dbt_params)


def _ingest_worldpop(destination, config, dbt_run, dbt_params):

    DBT_DIR = BASE_DIR / "dbt/worldpop"

    items = []

    for cc in config['options']['country_codes']:
        cc_3 = pycountry.countries.get(alpha_2=cc.upper()).alpha_3.lower()
        for item in config['items']:
            items.append(
                {
                    "url": get_worldpop_url(cc_3, item['theme']),
                    "file_name": os.path.basename(get_worldpop_url(cc_3, item['theme'])),
                    "table_name": item["theme"]
                }
            )

    temp_dir = tempfile.gettempdir()
    dlt_resource = worldpop(items, temp_dir)
    run_dlt(dlt_resource, destination, 'worldpop_raw')

    if dbt_run:
        if not dbt_params:
            dbt_params = [
                {
                    "name": '{}'.format(item["theme"]),
                    "alias": '{}'.format(item["theme"])
                }
                for item in config['items']
            ]
        run_dbt(destination, 'worldpop_stg', str(DBT_DIR), dbt_params)


def _ingest_geoboundaries(destination, config, dbt_run, dbt_params):

    DBT_DIR = BASE_DIR / "dbt/geobnd"

    items = []

    for cc in config['options']['country_codes']:
        cc_3 = pycountry.countries.get(alpha_2=cc.upper()).alpha_3
        for item in config['items']:
            items.append(
                {
                    "url": "https://www.geoboundaries.org/api/current/gbOpen/{}/{}/".format(cc_3, item["admin_level"]),
                    "table_name": 'geoboundaries__{}'.format(item["admin_level"].lower())
                }
            )

    dlt_resource = geoboundaries(items)
    run_dlt(dlt_resource, destination, 'geobnd_raw')

    if dbt_run:
        if not dbt_params:
            dbt_params = [
                {
                    "name": 'geoboundaries__{}'.format(item["admin_level"].lower()),
                    "alias": 'geoboundaries__{}'.format(item["admin_level"].lower())
                }
                for item in config['items']
            ]
        run_dbt(destination, 'geobnd_stg', str(DBT_DIR), dbt_params)


def _ingest_osm(destination, config, dbt_run, dbt_params):

    DBT_DIR = BASE_DIR / "dbt/osm"

    items = []
    for cc in config['options']['country_codes']:
        for item in config['items']:
            items.append(
                {
                    "country_code": cc,
                    "tag": item["theme"],
                    "value": None,
                    "table_name": item["theme"],
                    "element_type": "node"
                }
            )

    temp_dir = tempfile.gettempdir()
    dlt_resource = osm(items, temp_dir)
    run_dlt(dlt_resource, destination, 'osm_raw')

    if dbt_run:
        if not dbt_params:
            dbt_params = [
                {
                    "name": item["theme"],
                    "alias": item["theme"]
                }
                for item in config['items']
            ]
        run_dbt(destination, 'osm_stg', str(DBT_DIR), dbt_params)


def _ingest_caller(destination, config, dbt_run, dbt_params):

    if config['provider'] == 'gtfs':
        _ingest_gtfs(destination, config, dbt_run, dbt_params)
    elif config['provider'] == 'overturemaps':
        _ingest_ovm(destination, config, dbt_run, dbt_params)
    elif config['provider'] == 'worldpop':
        _ingest_worldpop(destination, config, dbt_run, dbt_params)
    elif config['provider'] == 'geoboundaries':
        _ingest_geoboundaries(destination, config, dbt_run, dbt_params)
    elif config['provider'] == 'openstreetmap':
        _ingest_osm(destination, config, dbt_run, dbt_params)
    else:
        raise ValueError('Data provider {} not supported.'.format(config['provider']))


def ingest_duckdb(duckdb_path: str, config: dict, dbt_run: bool=False, dbt_params: dict = None):

    destination = dlt.destinations.duckdb(duckdb_path)
    _ingest_caller(destination, config, dbt_run, dbt_params)


def ingest_motherduck(md_connect_string: str, config: dict, dbt_run: bool=False, dbt_params: dict = None):

    destination = dlt.destinations.motherduck(md_connect_string)
    _ingest_caller(destination, config, dbt_run, dbt_params)


def ingest_file(file_path: str, format: str, config: dict, dbt_run: bool=False, dbt_params: dict = None):

    destination = dlt.destinations.filesystem(
        bucket_url=file_path,
        
        # TODO nefunguje
        #file_format=format
    )

    _ingest_caller(destination, config, dbt_run, dbt_params)