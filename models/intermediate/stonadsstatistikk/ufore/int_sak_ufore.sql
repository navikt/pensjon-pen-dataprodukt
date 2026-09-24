{{ config(materialized='table') }}

with
ref_sak as (
    select
        sak_id,
        person_id,
        k_sak_t,
        k_sak_s,
        dato_opprettet,
        dato_endret,
        k_utlandstilknytning
    from {{ ref('stg_t_sak') }}

    where k_sak_t = 'UFOREP'
),

ref_person as (
    select
        person_id,
        fnr_fk,
        dato_fodsel
    from {{ ref('stg_t_person') }}
),

ref_saksdatoer as (
    select
        sak_id,
        har_ikke_sammenhengende_vedtak,
        forste_dato_virk_fom,
        siste_dato_lopende_tom

    from {{ ref('int_saksdatoer_fra_vedtak_ufore') }}
),

join_saksdatoer as (
    select
        s.*,
        har_ikke_sammenhengende_vedtak,
        forste_dato_virk_fom,
        siste_dato_lopende_tom        
    
    from ref_sak s
    
    left join ref_saksdatoer
        on s.sak_id = ref_saksdatoer.sak_id

),

join_person as (
    select
        join_saksdatoer.*,
        {{ kjonn_fra_fnr("ref_person.fnr_fk") }} as kjonn,
        extract(year from ref_person.dato_fodsel) as fodselsaar
    
    from join_saksdatoer

    inner join ref_person
        on join_saksdatoer.person_id = ref_person.person_id
)


select 
    sak_id,
    k_sak_t,
    k_sak_s,
    dato_opprettet,
    dato_endret,
    k_utlandstilknytning,
    har_ikke_sammenhengende_vedtak,
    forste_dato_virk_fom,
    siste_dato_lopende_tom,
    kjonn,
    fodselsaar

from join_person
