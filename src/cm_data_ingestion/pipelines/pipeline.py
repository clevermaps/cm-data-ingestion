from pathlib import Path
import os
import tempfile
import pycountry

import dlt

from .helpers import run_dbt, run_dlt, get_worldpop_url

from ..sources.gtfs.mobilitydatabase import source as gtfs_source
from ..sources.overturemaps import source as ovm_source
from ..sources.worldpop import source as worldpop_source
from ..sources.geoboundaries import source as geobnd_source
from ..sources.openstreetmap import source as osm_source


BASE_DIR = Path(__file__).parent


def ingest_gtfs(destination, config):

    dlt_resource = gtfs_source(config['items'])
    result = run_dlt(dlt_resource, destination, 'gtfs_raw')

    return result

# TODO build dbt_params
def stage_gtfs(destination, config, dbt_params):

    dbt_dir = BASE_DIR / "dbt/gtfs"

    run_dbt(destination, 'gtfs_stg', str(dbt_dir), dbt_params)


def ingest_ovm(destination, config):

    dlt_resource = ovm_source(config['items'], config['options'])
    result = run_dlt(dlt_resource, destination, 'ovm_raw')

    return result


def stage_ovm(destination, config, dbt_params):

    dbt_dir = BASE_DIR / "dbt/worldpop"

    if not dbt_params:
        dbt_params = [
            {
                "name": '{}__{}'.format(item["theme"], item["type"]),
                "alias": '{}__{}'.format(item["theme"], item["type"])
            }
            for item in config['items']
        ]
    run_dbt(destination, 'ovm_stg', str(dbt_dir), dbt_params)


def ingest_worldpop(destination, config):

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
    dlt_resource = worldpop_source(items, temp_dir)
    result = run_dlt(dlt_resource, destination, 'worldpop_raw')

    return result


def stage_worldpop(destination, config, dbt_params):

    dbt_dir = BASE_DIR / "dbt/worldpop"

    if not dbt_params:
        dbt_params = [
            {
                "name": '{}'.format(item["theme"]),
                "alias": '{}'.format(item["theme"])
            }
            for item in config['items']
        ]
    run_dbt(destination, 'worldpop_stg', str(dbt_dir), dbt_params)


def stage_geoboundaries(destination, config, dbt_params):

    DBT_DIR = BASE_DIR / "dbt/geobnd"

    if not dbt_params:
        dbt_params = [
            {
                "name": 'geoboundaries__{}'.format(item["admin_level"].lower()),
                "alias": 'geoboundaries__{}'.format(item["admin_level"].lower())
            }
            for item in config['items']
        ]

    run_dbt(destination, 'geobnd_stg', str(DBT_DIR), dbt_params)


def ingest_geoboundaries(destination, config):

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

    dlt_resource = geobnd_source(items)
    result = run_dlt(dlt_resource, destination, 'geobnd_raw')

    return result


def ingest_osm(destination, config):

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
    dlt_resource = osm_source(items, temp_dir)
    result = run_dlt(dlt_resource, destination, 'osm_raw')

    return result


def stage_osm(destination, config, dbt_params):

    dbt_dir = BASE_DIR / "dbt/osm"

    if not dbt_params:
        dbt_params = [
            {
                "name": item["theme"],
                "alias": item["theme"]
            }
            for item in config['items']
        ]
    run_dbt(destination, 'osm_stg', str(dbt_dir), dbt_params)


def ingest_caller(destination, config):

    if config['provider'] == 'gtfs':
        result = ingest_gtfs(destination, config)
    elif config['provider'] == 'overturemaps':
        result = ingest_ovm(destination, config)
    elif config['provider'] == 'worldpop':
        ingest_worldpop(destination, config)
    elif config['provider'] == 'geoboundaries':
        result = ingest_geoboundaries(destination, config)
    elif config['provider'] == 'openstreetmap':
        result = ingest_osm(destination, config)
    else:
        raise ValueError('Data provider {} not supported.'.format(config['provider']))
    
    return result


def stage_caller(destination, config, dbt_params):

    if config['provider'] == 'gtfs':
        stage_gtfs(destination, config, dbt_params)
    elif config['provider'] == 'overturemaps':
        stage_ovm(destination, config, dbt_params)
    elif config['provider'] == 'worldpop':
        stage_worldpop(destination, config, dbt_params)
    elif config['provider'] == 'geoboundaries':
        stage_geoboundaries(destination, config, dbt_params)
    elif config['provider'] == 'openstreetmap':
        stage_osm(destination, config, dbt_params)
    else:
        raise ValueError('Data provider {} not supported.'.format(config['provider']))


def ingest_duckdb(duckdb_path: str, config: dict, dbt_run: bool=False, dbt_params: dict = None):

    destination = dlt.destinations.duckdb(duckdb_path)
    ingest_caller(destination, config, dbt_run, dbt_params)

    if dbt_run: stage_caller(destination, config, dbt_params)


def ingest_motherduck(md_token: str, md_database: str, config: dict, dbt_run: bool=False, dbt_params: dict = None):

    destination = dlt.destinations.motherduck(f'md:{md_database}?motherduck_token={md_token}')
    ingest_caller(destination, config, dbt_run, dbt_params)

    if dbt_run: stage_caller(destination, config, dbt_params)


def ingest_file(file_path: str, config: dict, dbt_run: bool=False, dbt_params: dict = None):

    destination = dlt.destinations.filesystem(bucket_url=file_path)
    ingest_caller(destination, config, dbt_run, dbt_params)

    if dbt_run: stage_caller(destination, config, dbt_params)