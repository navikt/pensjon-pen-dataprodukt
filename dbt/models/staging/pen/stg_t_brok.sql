-- stg_t_vilkar_vedtak

select
    brok_id,
    teller,
    nevner
from {{ source('pen', 't_brok') }}
