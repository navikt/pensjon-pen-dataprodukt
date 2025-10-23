-- stg_t_vilkar_vedtak

select
    f_min_pen_niva_id,
    min_pen_niva_id
from {{ source('pen', 't_f_min_pen_niva') }}
