with tmp as
(
    select
        *
    from {{ source('osm', 'amenity') }}
)

select
    *,
    st_point(cast(lon as double precision), cast(lat as double precision)) as geom
from tmp