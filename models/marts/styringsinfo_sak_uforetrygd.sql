{{
  config(
    materialized = 'table',
    )
}}

with

ref_int_lopende_vedtak_ufore as (
    select
        sak_id,
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
    from {{ ref('int_lopende_vedtak_ufore') }}
)

select
    sak_id,
    k_sak_t,
    kjonn,
    fodselsaar,
    k_utlandstilknytning,
    sak_dato_opprettet,
    cast(null as date) as avsluttet_dato,
    cast(null as varchar(255)) as avsluttet_arsak,
    forste_dato_virk_fom as gyldig_fra_dato,
    cast(null as date) as gyldig_til_dato,
    cast(systimestamp at time zone 'UTC' as timestamp(9)) as kjoretidspunkt
from ref_int_lopende_vedtak_ufore
