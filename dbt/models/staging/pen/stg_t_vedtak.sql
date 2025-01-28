-- stg_t_vedtak

select
    vedtak_id,
    sak_id,
    kravhode_id,
    dato_lopende_fom,
    dato_lopende_tom
from {{ source('pen', 't_vedtak') }}
