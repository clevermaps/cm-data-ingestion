with tmp as
(
    select
        *
    from {{ source('geobnd', 'geoboundaries__adm5') }}
)

select
    *,
    st_geomfromtext(geometry) as geom
from tmp