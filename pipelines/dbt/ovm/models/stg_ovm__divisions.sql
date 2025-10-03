with tmp as
(
    select
        *
    from {{ source('ovm', 'divisions__division') }}
)

select
    id,
    st_geomfromwkb(geometry) as geom,
    cast(names.primary as varchar) as name,
    country,
    subtype,
    class
from tmp
where try(st_geomfromwkb(geometry)) is not null