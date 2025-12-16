{%- set relation = source('gtfs', 'routes') -%}
{%- set columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | list -%}

select
    {% if '_route_id' in columns -%}
        _route_id as route_id
    {% else %}
        route_id as route_id
    {% endif %},
    agency_id,
    route_short_name,
    route_long_name,
    --route_desc,
    route_type
    --route_url,
    --route_color,
    --route_text_color,
    --route_sort_order
from {{ source('gtfs', 'routes') }}