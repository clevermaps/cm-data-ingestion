import dlt
from pathlib import Path
import os
import tempfile
import duckdb

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
        run_dbt(destination, 'gtfs_dbt', str(DBT_DIR), dbt_params)


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
        run_dbt(destination, 'ovm_dbt', str(DBT_DIR), dbt_params)


def _ingest_worldpop(destination, config, dbt_run, dbt_params):

    DBT_DIR = BASE_DIR / "dbt/worldpop"

    items = [
        {
            "url": get_worldpop_url(item['country'], item['theme']),
            "file_name": os.path.basename(get_worldpop_url(item['country'], item['theme'])),
            "table_name": item["theme"]
        }
        for item in config['items']
    ]

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
        run_dbt(destination, 'worldpop_dbt', str(DBT_DIR), dbt_params)


def _ingest_geoboundaries(destination, config, dbt_run, dbt_params):

    DBT_DIR = BASE_DIR / "dbt/geobnd"

    items = [
        {
            "country_code": item["country_code"],
            "admin_level": item["admin_level"],
            "table_name": 'geoboundaries__{}'.format(item["admin_level"].lower())
        }
        for item in config['items']
    ]

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
        run_dbt(destination, 'geobnd_dbt', str(DBT_DIR), dbt_params)


def _ingest_osm(destination, config, dbt_run, dbt_params):

    DBT_DIR = BASE_DIR / "dbt/osm"

    items = [
        {
            "tag": item["theme"],
            "value": None,
            "country_codes": [item['country_code']],
            "table_name": item["theme"],
            "element_type": "node"
        }
        for item in config['items']
    ]

    temp_dir = tempfile.gettempdir()
    options = {
        'temp_dir': temp_dir
    }
    dlt_resource = osm(items, options)
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
        run_dbt(destination, 'osm_dbt', str(DBT_DIR), dbt_params)


def _ingest_caller(destination, config, dbt_run, dbt_params):

    if config['provider'] == 'gtfs':
        _ingest_gtfs(destination, config, dbt_run, dbt_params)
    elif config['provider'] == 'overturemaps':
        _ingest_ovm(destination, config, dbt_run, dbt_params)
    elif config['provider'] == 'worldpop':
        _ingest_worldpop(destination, config, dbt_run, dbt_params)
    elif config['provider'] == 'geoboundaries':
        _ingest_geoboundaries(destination, config, dbt_run, dbt_params)
    # TODO fix high CPU usage
    # elif config['provider'] == 'openstreetmap':
    #     _ingest_osm(destination, config, dbt_run, dbt_params)
    else:
        raise ValueError('Data provider {} not supported.'.format(config['provider']))


def ingest_duckdb(duckdb_path: str, config: dict, dbt_run: bool=False, dbt_params: dict = None):

    db = duckdb.connect(duckdb_path, config = {'threads': 1})

    destination = dlt.destinations.duckdb(db)
    _ingest_caller(destination, config, dbt_run, dbt_params)


def ingest_motherduck(md_connect_string: str, config: dict, dbt_run: bool=False, dbt_params: dict = None):

    destination = dlt.destinations.motherduck(md_connect_string)
    _ingest_caller(destination, config, dbt_run, dbt_params)