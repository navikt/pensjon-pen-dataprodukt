-- stg_t_vedtak

select
    vedtak_id,
    sak_id,
    person_id,
    kravhode_id,
    k_sak_t,
    k_vedtak_t,
    k_vedtak_s,
    versjon,
    dato_endret,
    dato_virk_fom,
    dato_virk_tom,
    dato_lopende_fom,
    dato_lopende_tom
from {{ source('pen', 't_vedtak') }}
