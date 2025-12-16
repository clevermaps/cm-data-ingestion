with tmp as
(
    select
        *
    from {{ source('geobnd', 'geoboundaries__adm2') }}
)

select
    *,
    st_geomfromtext(geometry) as geom
from tmp