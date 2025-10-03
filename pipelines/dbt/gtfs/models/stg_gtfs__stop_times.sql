with tmp as
(
    select
        *
    from {{ source('gtfs', 'stop_times') }}
)

select
    trip_id,
    arrival_time,
    departure_time,
    stop_id,
    stop_sequence,
    stop_headsign,
    pickup_type,
    drop_off_type,
    shape_dist_traveled,
    timepoint
from tmp