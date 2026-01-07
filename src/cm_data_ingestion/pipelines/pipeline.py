from pathlib import Path
import os
import tempfile
import pycountry
import uuid

import dlt

import logging

from ..sources.gtfs.mobilitydatabase import source as gtfs_source
from ..sources.overturemaps import source as ovm_source
from ..sources.worldpop import source as worldpop_source
from ..sources.geoboundaries import source as geobnd_source
from ..sources.openstreetmap import source as osm_source


BASE_DIR = Path(__file__).parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    force=True,
)

logger = logging.getLogger(__name__)

def run_dlt(dlt_resource, destination, schema):

    logger.info("Starting run_dlt with destination: %s, schema: %s", destination, schema)

    pipeline_name = str(uuid.uuid4())
    logger.info("Creating pipeline with name: %s", pipeline_name)

    pipeline = dlt.pipeline(
        pipeline_name=str(uuid.uuid4()),
        destination=destination, 
        dataset_name=schema
    )

    logger.info("Pipeline created successfully")

    logger.info("Running pipeline with resource: %s", dlt_resource.name)
    result = pipeline.run(dlt_resource, write_disposition='replace')
    logger.info("Pipeline run completed with result: %s", result)
    
    return result

def ingest_gtfs(destination, config):

    dlt_resource = gtfs_source(config['items'])
    result = run_dlt(dlt_resource, destination, 'dlt_gtfs')

    return result


def ingest_ovm(destination, config):

    dlt_resource = ovm_source(config['items'], config['options'])
    result = run_dlt(dlt_resource, destination, 'dlt_ovm')

    return result



def ingest_worldpop(destination, config):

    items = []

    for cc in config['options']['country_codes']:
        cc_3 = pycountry.countries.get(alpha_2=cc.upper()).alpha_3.lower()
        for item in config['items']:
            items.append(
                 {
                    "country": cc_3,
                    "theme": item['theme'],
                    "table_name": item["theme"]
                }
            )

    temp_dir = tempfile.gettempdir()
    dlt_resource = worldpop_source(items, temp_dir)
    result = run_dlt(dlt_resource, destination, 'dlt_worldpop')

    return result


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
    result = run_dlt(dlt_resource, destination, 'dlt_geobnd')

    return result


def ingest_osm(destination, config):

    items = []
    for cc in config['options']['country_codes']:
        for item in config['items']:
            items.append(
                {
                    "country_code": cc,
                    "tag": item["theme"],
                    "value": item.get('type', None),
                    "table_name": item["theme"]
                }
            )

    temp_dir = tempfile.gettempdir()
    dlt_resource = osm_source(items, temp_dir)
    result = run_dlt(dlt_resource, destination, 'dlt_osm')

    return result


def ingest_caller(destination, config):

    if config['provider'] == 'gtfs':
        result = ingest_gtfs(destination, config)
    elif config['provider'] == 'overturemaps':
        result = ingest_ovm(destination, config)
    elif config['provider'] == 'worldpop':
        result = ingest_worldpop(destination, config)
    elif config['provider'] == 'geoboundaries':
        result = ingest_geoboundaries(destination, config)
    elif config['provider'] == 'openstreetmap':
        result = ingest_osm(destination, config)
    else:
        raise ValueError('Data provider {} not supported.'.format(config['provider']))
    
    return result


def ingest_duckdb(duckdb_path: str, config: dict):

    destination = dlt.destinations.duckdb(duckdb_path)
    ingest_caller(destination, config)


def ingest_motherduck(md_token: str, md_database: str, config: dict):

    destination = dlt.destinations.motherduck(f'md:{md_database}?motherduck_token={md_token}')
    ingest_caller(destination, config)


def ingest_file(file_path: str, config: dict):

    destination = dlt.destinations.filesystem(bucket_url=file_path)
    ingest_caller(destination, config)