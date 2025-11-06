{%- set relation = source('gtfs', 'trips') -%}
{%- set columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | list -%}

select
    {% if '_route_id' in columns -%}
        _route_id as route_id
    {% else %}
        route_id as route_id
    {% endif %},
    service_id,
    trip_id,
    trip_headsign,
    --trip_short_name,
    direction_id,
    block_id,
    --shape_id,
    wheelchair_accessible
    --bikes_allowed
from {{ source('gtfs', 'trips') }}