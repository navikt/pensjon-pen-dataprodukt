-- stg_t_pen_under_utbet

select
    pen_under_utbet_id,
    total_belop_brutto,
    total_belop_netto
from {{ source('pen', 't_pen_under_utbet') }}
