import dlt
from .helpers import get_data


@dlt.source(name="openstreetmap")
def osm(configs, temp_dir):
    """
    configs: list[dict], kde každý dict má country_code, tag, value a dalsi volitelne parametry
    """
    for cfg in configs:
        print(cfg)

        for cc in cfg["country_codes"]:
            tag = cfg["tag"]
            value = cfg["value"]
            table_name = cfg['table_name']
            yield dlt.resource(
                get_data(
                    temp_dir,
                    cc, 
                    tag, 
                    value, 
                    cfg.get("element_type", None), 
                    cfg.get("target_date_range", None), 
                    cfg.get("target_date_tolerance_days", 0), 
                    cfg.get("prefer_older", False)
                ),
                name=f'{table_name}__{cc}'
            )