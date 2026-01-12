-- behandlingsstatistikk_meldinger
-- view som mapper over kolonnenavn fra pen til det team sak ønsker

{{
    config(
        materialized='view',
    )
}}


with

ref_stg_t_person as (
    select
        person_id,
        fnr_fk
    from {{ ref('stg_t_person') }}
),

ref_stg_t_sak as (
    select
        sak_id,
        k_sak_t,
        person_id
    from {{ ref('stg_t_sak') }}
),

ref_behandlingsstatistikk_grunnlag as (
    select
        sak_id, -- kh
        kravhode_id, -- kh
        behandling_resultat,
        k_krav_gjelder, -- kh
        k_krav_s, -- kh
        k_krav_arsak_t, -- ka
        k_behandling_t, -- kh
        k_utlandstilknytning, -- sak
        ansvarlig_enhet, -- kh
        endret_av, -- kh
        opprettet_av, -- kh
        attesterer, -- vedtak
        dato_opprettet, -- kh
        dato_onsket_virk, -- kh
        dato_mottatt_krav, -- kh
        dato_virk_fom, -- v
        dato_endret, -- kh
        ferdigbehandlet_tid,
        kravhode_id_for, -- kh
        kjoretidspunkt
    from {{ ref('snapshot_saksbehandlingsstatistikk') }}
),

join_fnr as (
    select
        beh.*,
        s.k_sak_t,
        person.fnr_fk
    from ref_behandlingsstatistikk_grunnlag beh
    left join ref_stg_t_sak s on beh.sak_id = s.sak_id
    left join ref_stg_t_person person on s.person_id = person.person_id
),

nye_kolonnenavn as (
    select
        kravhode_id as behandling_id,
        sak_id,
        fnr_fk as aktor_id,
        k_sak_t as sak_ytelse,
        k_utlandstilknytning as sak_utland,
        k_krav_gjelder as behandling_type,
        k_krav_s as behandling_status,
        behandling_resultat,
        k_behandling_t as behandling_metode,
        k_krav_arsak_t as behandling_arsak,
        opprettet_av,
        endret_av as saksbehandler,
        attesterer as ansvarlig_beslutter,
        ansvarlig_enhet,
        dato_mottatt_krav as mottatt_tid,
        dato_opprettet as registrert_tid,
        ferdigbehandlet_tid,
        dato_virk_fom as utbetalt_tid,
        dato_endret as endret_tid,
        dato_onsket_virk as forventetoppstart_tid,
        kjoretidspunkt as teknisk_tid,
        'PESYS' as fagsystem_navn,
        kravhode_id_for as relatertbehandling_id,
        'PESYS' as relatert_fagsystem,
        '-1' as tilbakekrev_belop,
        '-1' as funksjonell_periode_fom,
        '-1' as funksjonell_periode_tom,
        '-1' as fagsystem_versjon
    from join_fnr
)

select * from nye_kolonnenavn
