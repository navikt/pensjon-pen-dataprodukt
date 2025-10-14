-- stg_t_min_pen_niva

select
    min_pen_niva_id,
    sats
from {{ source('pen', 't_min_pen_niva') }}
