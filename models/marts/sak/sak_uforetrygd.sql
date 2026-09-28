{{
  config(
    materialized = 'table',
    )
}}

with
ref_int_lopende_vedtak_ufore as (
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
    from {{ ref('int_sak_ufore') }}
)

select
    sak_id,
    k_sak_t,
    kjonn,
    fodselsaar,
    k_utlandstilknytning,
    dato_opprettet as sak_dato_opprettet,
    cast(null as date) as avsluttet_dato,
    cast(null as varchar(255)) as avsluttet_arsak,
    forste_dato_virk_fom as gyldig_fra_dato,
    siste_dato_lopende_tom as gyldig_til_dato,
    har_ikke_sammenhengende_vedtak,
    cast(systimestamp at time zone 'UTC' as timestamp(9)) as kjoretidspunkt
from ref_int_lopende_vedtak_ufore
