-- int_behandling_grunnlag
-- Versjon 2 av et grunnlag for behandlingsstatistikk
-- denne går inn i et snapshot som sjekker endringer

{{
  config(
    materialized = 'table',
    )
}}

with

behandling_ferdig as (
    select * from {{ ref('int_behandling_ferdig') }}
    where k_krav_s = 'FERDIG'
),

behandling_avbrutt as (
    select * from {{ ref('int_behandling_avbrutt') }}
    where k_krav_s = 'AVBRUTT'
),

behandling_andre as (
    select * from {{ ref('int_behandling') }}
    where k_krav_s not in ('FERDIG', 'AVBRUTT')
),

union_behandling as (
    select
        sak_id, -- kh
        kravhode_id, -- kh
        k_krav_gjelder, -- kh
        k_krav_s, -- kh
        k_krav_arsak_t, -- ka
        k_behandling_t, -- kh
        k_utlandstilknytning, -- sak
        ansvarlig_enhet,
        endret_av,
        opprettet_av, -- kh
        attesterer, -- vedtak
        dato_opprettet, -- kh
        dato_onsket_virk, -- kh
        dato_mottatt_krav, -- kh
        dato_virk_fom, -- v
        dato_endret, -- kh
        dato_vedtak as ferdigbehandlet_tid,
        kravhode_id_for, -- kh
        k_vedtak_t,
        k_vedtak_s,
        k_vilkar_resul_t,
        vv__k_vilkar_resul_t,
        k_klageank_res_t,
        null as avbrutt_behandling_resultat
    from behandling_ferdig
    union all
    select
        sak_id, -- kh
        kravhode_id, -- kh
        k_krav_gjelder, -- kh
        k_krav_s, -- kh
        k_krav_arsak_t, -- ka
        k_behandling_t, -- kh
        k_utlandstilknytning, -- sak
        ansvarlig_enhet,
        endret_av,
        opprettet_av, -- kh
        null as attesterer,
        dato_opprettet, -- kh
        dato_onsket_virk, -- kh
        dato_mottatt_krav, -- kh
        null as dato_virk_fom, -- v
        dato_endret, -- kh
        trunc(dato_endret) as ferdigbehandlet_tid, --kh samme datatype some i behandling_ferdig
        kravhode_id_for, -- kh
        null as k_vedtak_t,
        null as k_vedtak_s,
        null as k_vilkar_resul_t,
        null as vv__k_vilkar_resul_t,
        null as k_klageank_res_t,
        avbrutt_behandling_resultat
    from behandling_avbrutt
    union all
    select
        sak_id, -- kh
        kravhode_id, -- kh
        k_krav_gjelder, -- kh
        k_krav_s, -- kh
        k_krav_arsak_t, -- ka
        k_behandling_t, -- kh
        k_utlandstilknytning, -- sak
        ansvarlig_enhet,
        endret_av,
        opprettet_av, -- kh
        null as attesterer,
        dato_opprettet, -- kh
        dato_onsket_virk, -- kh
        dato_mottatt_krav, -- kh
        null as dato_virk_fom, -- v
        dato_endret, -- kh
        null as ferdigbehandlet_tid,
        kravhode_id_for, -- kh
        null as k_vedtak_t,
        null as k_vedtak_s,
        null as k_vilkar_resul_t,
        null as vv__k_vilkar_resul_t,
        null as k_klageank_res_t,
        null as avbrutt_behandling_resultat
    from behandling_andre
)

select
    sak_id, -- kh
    kravhode_id, -- kh
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
    ferdigbehandlet_tid, -- v
    kravhode_id_for, -- kh
    k_vedtak_t, -- til behandling_resultat
    k_vedtak_s, -- til behandling_resultat
    k_vilkar_resul_t, -- til behandling_resultat
    vv__k_vilkar_resul_t, -- til behandling_resultat
    k_klageank_res_t, -- til behandling_resultat
    avbrutt_behandling_resultat,
    cast(systimestamp at time zone 'UTC' as timestamp(9)) as kjoretidspunkt -- brukes til last fra Oracle til BQ
from union_behandling
