-- som tabell???

select
    vedtak_id,
    sak_id,
    kravhode_id,
    k_sak_t,
    dato_lopende_fom,
    dato_lopende_tom
from {{ ref('stg_t_vedtak') }}
where
    k_sak_t = 'ALDER'
    and dato_lopende_fom <= {{ periode_sluttdato(var("periode")) }}
    and (dato_lopende_tom is null or dato_lopende_tom >= trunc({{ periode_sluttdato(var("periode")) }}))
