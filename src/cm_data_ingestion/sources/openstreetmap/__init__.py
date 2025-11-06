import dlt
from .helpers import get_data


@dlt.source(name="openstreetmap")
def osm(items, options):
    """
    configs: list[dict], kde každý dict má country_code, tag, value a dalsi volitelne parametry
    """
    for item in items:
        print(item)

        for cc in item["country_codes"]:
            tag = item["tag"]
            value = item["value"]
            table_name = item['table_name']
            
            yield dlt.resource(
                get_data(
                    options['temp_dir'],
                    cc, 
                    tag, 
                    value, 
                    item.get("element_type", None), 
                    item.get("target_date_range", None), 
                    item.get("target_date_tolerance_days", 0), 
                    item.get("prefer_older", False)
                ),
                name=table_name
            )