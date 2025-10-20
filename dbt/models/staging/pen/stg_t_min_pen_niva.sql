-- stg_t_min_pen_niva

select
    min_pen_niva_id,
    sats,
    pro_rata_teller_mnd,
    pro_rata_nevner_mnd
from {{ source('pen', 't_min_pen_niva') }}
