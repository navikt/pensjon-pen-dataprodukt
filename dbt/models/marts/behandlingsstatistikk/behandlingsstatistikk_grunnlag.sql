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

ref_stg_t_kode67_kravhode as (
    select kravhode_id from {{ ref('stg_t_kode67_kravhode') }}
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
),

-- maskere geolokaliserende felter for kode6/7
maskere_geolokalisering as (
    select
        beh.sak_id,
        beh.kravhode_id,
        beh.behandling_resultat,
        beh.k_krav_gjelder,
        beh.k_krav_s,
        beh.k_sak_s,
        beh.k_krav_arsak_t,
        beh.k_behandling_t,
        beh.k_utlandstilknytning,
        case
            when k67.kravhode_id is null then beh.opprettet_av else '-5'
        end as opprettet_av,
        -- case
        --     when k67.kravhode_id is null then beh.endret_av else '-5'
        -- end as endret_av,
        -- case
        --     when k67.kravhode_id is null then beh.ansvarlig_enhet else '-5'
        -- end as ansvarlig_enhet,
        beh.dato_opprettet,
        beh.dato_onsket_virk,
        beh.dato_mottatt_krav,
        beh.kravhode_id_for
    from union_behandling beh
    left join ref_stg_t_kode67_kravhode k67
        on beh.kravhode_id = k67.kravhode_id
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
from maskere_geolokalisering
