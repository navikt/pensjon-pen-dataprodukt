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
        dato_lopende_tom,
        dato_virk_fom
    from {{ ref('stg_t_vedtak') }}
    where
        k_sak_t = 'ALDER'
),

ref_kravhode as (
    select
        sak_id,
        kravhode_id,
        k_regelverk_t,
        k_afp_t
    from {{ ref('stg_t_kravhode') }}
),

siste_dato_virk_fom as (
    select
        v.*,
        first_value(v.dato_virk_fom) over (partition by v.sak_id order by v.dato_lopende_fom) as forste_dato_virk_fom,
        first_value(v.dato_lopende_fom) over (partition by v.sak_id order by v.dato_lopende_fom) as forste_dato_lopende_fom
    from ref_vedtak v
    where
        v.dato_lopende_fom is not null
),

siste_lopende_vedtak as (
    select * from siste_dato_virk_fom
    where
        dato_lopende_fom <= {{ periode_sluttdato(var("periode")) }}
        and (dato_lopende_tom is null or dato_lopende_tom >= trunc({{ periode_sluttdato(var("periode")) }}))
),


join_kravhode as (
    select
        siste_lopende_vedtak.*,
        ref_kravhode.k_regelverk_t,
        ref_kravhode.k_afp_t
    from siste_lopende_vedtak
    inner join ref_kravhode
        on siste_lopende_vedtak.kravhode_id = ref_kravhode.kravhode_id
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
    dato_lopende_tom,
    forste_dato_virk_fom,
    forste_dato_lopende_fom
from join_kravhode
