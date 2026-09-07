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
        k_sak_t = 'UFOREP'
),

ref_kravhode as (
    select
        sak_id,
        kravhode_id,
        k_regelverk_t,
        k_afp_t
    from {{ ref('stg_t_kravhode') }}
),

ref_person as (
    select
        person_id,
        fnr_fk,
        dato_fodsel
    from {{ ref('stg_t_person') }}
),

ref_sak as (
    select
        sak_id,
        k_utlandstilknytning,
        dato_opprettet
    from {{ ref('stg_t_sak') }}
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
        dato_lopende_fom <= (systimestamp at time zone 'Europe/Oslo')
        and (dato_lopende_tom is null or dato_lopende_tom >= trunc((systimestamp at time zone 'Europe/Oslo')))

),


join_kravhode as (
    select
        siste_lopende_vedtak.*,
        ref_kravhode.k_regelverk_t,
        ref_kravhode.k_afp_t
    from siste_lopende_vedtak
    inner join ref_kravhode
        on siste_lopende_vedtak.kravhode_id = ref_kravhode.kravhode_id
),

join_person as (
    select
        join_kravhode.*,
        case
            when substr(ref_person.fnr_fk, 9, 1) in ('0', '2', '4', '6', '8')
                then 'K'
            when substr(ref_person.fnr_fk, 9, 1) in ('1', '3', '5', '7', '9')
                then 'M'
        end as kjonn,
        extract(year from ref_person.dato_fodsel) as fodselsaar,
        ref_sak.k_utlandstilknytning,
        ref_sak.dato_opprettet as sak_dato_opprettet
    from join_kravhode
    inner join ref_person
        on join_kravhode.person_id = ref_person.person_id
    inner join ref_sak
        on join_kravhode.sak_id = ref_sak.sak_id
)

select
    sak_id,
    person_id,
    vedtak_id,
    kravhode_id,
    kjonn,
    fodselsaar,
    k_utlandstilknytning,
    sak_dato_opprettet,
    k_sak_t,
    k_vedtak_s,
    k_vedtak_t,
    k_regelverk_t,
    k_afp_t,
    dato_lopende_fom,
    dato_lopende_tom,
    forste_dato_virk_fom,
    forste_dato_lopende_fom
from join_person
