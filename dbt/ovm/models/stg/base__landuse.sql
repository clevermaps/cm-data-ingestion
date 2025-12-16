select
    *,
    st_geomfromwkb(geometry) as geom
from {{ source('ovm', 'base__landuse') }}
where try(st_geomfromwkb(geometry)) is not null