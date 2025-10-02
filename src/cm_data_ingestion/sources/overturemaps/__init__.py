import dlt

from .helpers import get_data_bbox_arrow

# Overturemaps schema
# https://til.simonwillison.net/overture-maps/overture-maps-parquet


@dlt.source(name="overturemaps")
def ovm(configs, bbox, release):
    """
    configs: list[dict], kde každý dict má např. theme, type, bbox, release
    """
    for cfg in configs:
        print(cfg)
        ovm_theme = cfg["theme"]
        ovm_type = cfg["type"]
        table_name = cfg['table_name']
    
        # TODO max_table_nesting=0 to pipeline config
        yield dlt.resource(
            get_data_bbox_arrow(ovm_theme, ovm_type, bbox, release),
            name=f'{table_name}',
            max_table_nesting=0
        )

