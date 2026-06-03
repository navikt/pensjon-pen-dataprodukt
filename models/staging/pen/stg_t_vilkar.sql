-- stg_t_vilkar

select
    vilkar_id,
    vilkar_vedtak_id,
    -- k_vilkar_t,
    hoveddiagnose,
    bidiagnoser
from {{ source('pen', 't_vilkar') }}
