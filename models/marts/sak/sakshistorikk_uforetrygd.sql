{{
  config(
    materialized = 'table',
    )
}}

with
ref_int_sakshistorikk_ufore as (
    select
        sak_id,
        totalt_antall_perioder,
        periode_nr,
        er_siste_periode,
        forste_dato_lopende_fom,
        siste_dato_lopende_tom,
        forste_dato_virk_fom
    from {{ ref('int_sakshistorikk_ufore') }}
)

select
    sak_id,
    totalt_antall_perioder,
    periode_nr,
    er_siste_periode,
    forste_dato_lopende_fom,
    siste_dato_lopende_tom,
    forste_dato_virk_fom,
    cast(systimestamp at time zone 'UTC' as timestamp(9)) as kjoretidspunkt

from ref_int_sakshistorikk_ufore
