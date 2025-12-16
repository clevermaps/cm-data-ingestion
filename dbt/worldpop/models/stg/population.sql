

select
    row_number() over() as id,
    *,
    st_point(lon, lat) as geom,
    value as pop
from {{ source('worldpop', 'population') }}