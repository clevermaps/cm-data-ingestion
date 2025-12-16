{%- set relation = source('gtfs', 'stop_times') -%}
{%- set columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | list -%}

select
    {% if '_trip_id' in columns -%}
        _trip_id as trip_id
    {% else %}
        trip_id as trip_id
    {% endif %},
    arrival_time,
    departure_time,
    stop_id,
    cast(stop_sequence as integer) as stop_sequence,
    --stop_headsign,
    pickup_type,
    drop_off_type
    --shape_dist_traveled,
    --timepoint
from {{ source('gtfs', 'stop_times') }}