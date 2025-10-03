with tmp as
(
    select
        *
    from {{ source('ovm', 'places__place') }}
)

select
    id,
    st_geomfromwkb(geometry) as geom,
    names->>'$.primary' as name,
    categories->>'$.primary' as category,
    addresses[0] as address
from tmp
where try(st_geomfromwkb(geometry)) is not null