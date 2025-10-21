with tmp as
(
    select
        *
    from {{ source('geobnd', 'geoboundaries') }}
)

select
    *
from tmp