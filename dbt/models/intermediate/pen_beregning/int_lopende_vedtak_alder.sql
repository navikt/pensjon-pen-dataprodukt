-- int_lopende_vedtak_alder

{{ config(materialized='table') }} -- table gjør at spørringene videre i dbt-løpet får samme sett med vedtak

with

ref_vedtak as (
    select
        sak_id,
        person_id,
        vedtak_id,
        kravhode_id,
        k_sak_t,
        k_vedtak_s,
        k_vedtak_t,
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
        k_regelverk_t,
        k_afp_t
    from {{ ref('stg_t_kravhode') }}
),

join_kravhode as (
    select
        ref_vedtak.*,
        ref_kravhode.k_regelverk_t,
        ref_kravhode.k_afp_t
    from ref_vedtak
    inner join ref_kravhode
        on ref_vedtak.kravhode_id = ref_kravhode.kravhode_id
)

select
    sak_id,
    person_id,
    vedtak_id,
    kravhode_id,
    k_sak_t,
    k_vedtak_s,
    k_vedtak_t,
    k_regelverk_t,
    k_afp_t,
    dato_lopende_fom,
    dato_lopende_tom
from join_kravhode
