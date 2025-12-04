
select
    id,
    st_geomfromwkb(geometry) as geom,
    names->>'primary' as name,
    country,
    subtype,
    class
from {{ source('ovm', 'divisions__division') }}
where try(st_geomfromwkb(geometry)) is not null