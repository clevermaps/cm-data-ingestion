import dlt
from .helpers import get_data, find_suitable_pbf_files


# TODO logging

@dlt.resource(max_table_nesting=0)
def osm_resource(country_code, tag, value, element_type=None, target_date_range=None, target_date_tolerance_days=0, prefer_older=False):
    yield from get_data(country_code, tag, value, element_type, target_date_range, target_date_tolerance_days, prefer_older)

def get_available_data_versions(country_code, target_date_range=None, target_date_tolerance_days=0):
    """
    Get available data versions for a specific country code.
    This function returns a list of available data versions, which can be used to filter or select specific data versions.
    Each data version is represented as a string in the format "YYYY-MM-DD".
    """
    pbf_files = find_suitable_pbf_files(
        country_code,
        target_date_range=target_date_range,
        target_date_tolerance_days=target_date_tolerance_days
    )
    return [file_date.strftime("%Y-%m-%d") for file_date, _, _ in pbf_files if file_date is not None]