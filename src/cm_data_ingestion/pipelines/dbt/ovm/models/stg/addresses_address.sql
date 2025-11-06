with tmp as
(
    select
        *
    from {{ source('ovm', 'addresses_address') }}
)

select
    id,
    st_geomfromwkb(geometry) as geom,
    postcode,
    street,
    number
from tmp
where try(st_geomfromwkb(geometry)) is not null