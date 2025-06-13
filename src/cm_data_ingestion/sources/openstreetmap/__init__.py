import dlt
from .helpers import get_data, find_suitable_pbf_files


# TODO logging

@dlt.resource(max_table_nesting=0)
def osm_resource(country_code, tag, value, element_type=None, target_date_range=None, target_date_tolerance_days=0):
    yield from get_data(country_code, tag, value, element_type, target_date_range, target_date_tolerance_days)

def get_available_data_versions(country_code, target_date_range=None, target_date_tolerance_days=0):
    """
    Get available data versions for a specific country code.
    This function returns a list of available data versions, which can be used to filter or select specific data versions.
    """
    pbf_files = find_suitable_pbf_files(
        country_code,
        target_date_range=target_date_range,
        target_date_tolerance_days=target_date_tolerance_days
    )
    return [date_str for file_url, date_str in pbf_files if date_str is not None]