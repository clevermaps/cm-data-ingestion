{%- set relation = source('gtfs', 'stops') -%}
{%- set columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | list -%}

select
    {% if '_stop_id' in columns -%}
        _stop_id as stop_id
    {% else %}
        stop_id as stop_id
    {% endif %},
    --stop_code,
    stop_name,
    --stop_desc,
    stop_lat,
    stop_lon,
    zone_id,
    location_type,
    parent_station,
    --stop_timezone,
    wheelchair_boarding,
    --level_id,
    platform_code,
    st_point(cast(stop_lon as double precision), cast(stop_lat as double precision)) as geom
from {{ source('gtfs', 'stops') }}