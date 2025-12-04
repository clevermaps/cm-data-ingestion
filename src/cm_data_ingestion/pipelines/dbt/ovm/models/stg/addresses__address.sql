select
    id,
    st_geomfromwkb(geometry) as geom,
    postcode,
    street,
    number
from {{ source('ovm', 'addresses__address') }}
where try(st_geomfromwkb(geometry)) is not null