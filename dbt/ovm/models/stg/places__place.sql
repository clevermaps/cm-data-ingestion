select
    id,
    st_geomfromwkb(geometry) as geom,
    names->>'$.primary' as name,
    categories->>'$.primary' as category,
    addresses[0] as address
from {{ source('ovm', 'places__place') }}
where try(st_geomfromwkb(geometry)) is not null