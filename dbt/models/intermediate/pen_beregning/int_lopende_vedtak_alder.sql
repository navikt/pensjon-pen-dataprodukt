-- som tabell???
with

ref_vedtak as (
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
),

ref_kravhode as (
    select
        sak_id,
        kravhode_id,
        k_regelverk_t
    from {{ ref('stg_t_kravhode') }}
),

join_kravhode as (
    select
        ref_vedtak.*,
        ref_kravhode.k_regelverk_t
    from ref_vedtak
    inner join ref_kravhode
        on ref_vedtak.kravhode_id = ref_kravhode.kravhode_id
),

select
    vedtak_id,
    sak_id,
    kravhode_id,
    k_sak_t,
    dato_lopende_fom,
    dato_lopende_tom,
    k_regelverk_t
from join_kravhode
