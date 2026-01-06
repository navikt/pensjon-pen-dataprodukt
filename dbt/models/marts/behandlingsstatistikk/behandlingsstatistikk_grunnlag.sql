-- behandlingsstatistikk_grunnlag
-- Grunnlag for behandlingsstatistikk, samt maskering av geolokaliserende felter for kode6/7
-- Nye feltnavn gis i behandlingsstatistikk_meldinger

{{
    config(
        materialized='table',
    )
}}

with

behandling_ferdig as (
    select * from {{ ref('int_behandling_ferdig') }}
    -- herfra må vi også hente noen datoer, feks dato_vedtatt
),

behandling_avbrutt as (
    select * from {{ ref('int_behandling_avbrutt') }}
),

behandling_andre as (
    select * from {{ ref('int_behandling') }}
    where k_krav_s not in ('FERDIG', 'AVBRUTT')
),

union_behandling as (
    select
        sak_id, -- kh
        kravhode_id, -- kh
        behandling_resultat,
        k_krav_gjelder, -- kh
        k_krav_s, -- kh
        k_sak_s, -- sak
        k_krav_arsak_t, -- ka
        k_behandling_t, -- kh
        k_utlandstilknytning, -- sak
        opprettet_av, -- kh
        dato_opprettet, -- kh
        dato_onsket_virk, -- kh
        dato_mottatt_krav, -- kh
        kravhode_id_for -- kh
    from behandling_ferdig
    union all
    select
        sak_id, -- kh
        kravhode_id, -- kh
        behandling_resultat,
        k_krav_gjelder, -- kh
        k_krav_s, -- kh
        k_sak_s, -- sak
        k_krav_arsak_t, -- ka
        k_behandling_t, -- kh
        k_utlandstilknytning, -- sak
        opprettet_av, -- kh
        dato_opprettet, -- kh
        dato_onsket_virk, -- kh
        dato_mottatt_krav, -- kh
        kravhode_id_for -- kh
    from behandling_avbrutt
    union all
    select
        sak_id, -- kh
        kravhode_id, -- kh
        null as behandling_resultat,
        k_krav_gjelder, -- kh
        k_krav_s, -- kh
        k_sak_s, -- sak
        k_krav_arsak_t, -- ka
        k_behandling_t, -- kh
        k_utlandstilknytning, -- sak
        opprettet_av, -- kh
        dato_opprettet, -- kh
        dato_onsket_virk, -- kh
        dato_mottatt_krav, -- kh
        kravhode_id_for -- kh
    from behandling_andre
)

select
    sak_id, -- kh
    kravhode_id, -- kh
    behandling_resultat,
    k_krav_gjelder, -- kh
    k_krav_s, -- kh
    k_sak_s, -- sak
    k_krav_arsak_t, -- ka
    k_behandling_t, -- kh
    k_utlandstilknytning, -- sak
    opprettet_av, -- kh
    dato_opprettet, -- kh
    dato_onsket_virk, -- kh
    dato_mottatt_krav, -- kh
    kravhode_id_for -- kh
from union_behandling
