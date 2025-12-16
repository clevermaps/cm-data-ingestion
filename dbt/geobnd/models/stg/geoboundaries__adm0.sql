with tmp as
(
    select
        *
    from {{ source('geobnd', 'geoboundaries__adm0') }}
)

select
    *,
    st_geomfromtext(geometry) as geom
from tmp