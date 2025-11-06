with tmp as
(
    select
        *
    from {{ source('ovm', 'divisions_division') }}
)

select
    id,
    st_geomfromwkb(geometry) as geom,
    names->>'primary' as name,
    country,
    subtype,
    class
from tmp
where try(st_geomfromwkb(geometry)) is not null