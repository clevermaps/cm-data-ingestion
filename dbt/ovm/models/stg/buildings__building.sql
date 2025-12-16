select
    *,
    st_geomfromwkb(geometry) as geom
from {{ source('ovm', 'buildings__building') }}
where try(st_geomfromwkb(geometry)) is not null