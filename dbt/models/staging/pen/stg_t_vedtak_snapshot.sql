-- stg_t_vedtak_snapshot

select
    sak_id,
    vedtak_id,
    dato_endret,
    dato_lopende_fom,
    dato_lopende_tom
from {{ ref('stg_t_vedtak') }}
