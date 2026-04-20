-- stg_t_vilkar_vedtak

select
    vilkar_vedtak_id,
    vedtak_id,
    kravlinje_id,
    k_vilkar_resul_t,
    k_vilk_vurd_t,
    k_kravlinje_t,
    dato_virk_fom,
    dato_virk_tom
from {{ source('pen', 't_vilkar_vedtak') }}
