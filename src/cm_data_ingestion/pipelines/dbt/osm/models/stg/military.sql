select
    *,
    st_point(cast(lon as double precision), cast(lat as double precision)) as geom
from {{ source('osm', 'military') }}