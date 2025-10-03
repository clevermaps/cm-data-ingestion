

select
    *,
    st_point(lon, lat) as geom,
    value as pop
from {{ source('worldpop', 'population') }}