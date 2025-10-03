with tmp as
(
    select
        *
    from {{ source('gtfs', 'stops') }}
)

select
    stop_id,
    stop_code,
    stop_name,
    stop_desc,
    stop_lat,
    stop_lon,
    zone_id,
    location_type,
    parent_station,
    stop_timezone,
    wheelchair_boarding,
    level_id,
    platform_code
from tmp