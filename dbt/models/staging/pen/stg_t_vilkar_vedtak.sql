-- stg_t_vilkar_vedtak

select
    vedtak_id,
    k_vilk_vurd_t,
    dato_virk_fom,
    dato_virk_tom
from {{ source('pen', 't_vilkar_vedtak') }}
