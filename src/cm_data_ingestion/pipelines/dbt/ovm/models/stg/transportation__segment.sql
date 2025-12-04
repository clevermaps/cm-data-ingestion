select
    *,
    st_geomfromwkb(geometry) as geom
from {{ source('ovm', 'transportation__segment') }}
where try(st_geomfromwkb(geometry)) is not null