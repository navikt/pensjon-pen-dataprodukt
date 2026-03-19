-- behandlingsstatistikk_grunnlag
-- Grunnlag for behandlingsstatistikk, som blir snapshottet av dbt
-- Etter snapshot velger vi ut hva som skal bli meldinger til team Sak
-- Nye feltnavn gis i behandlingsstatistikk_meldinger

{{
    config(
        materialized='table',
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
        'placeholder' as behandling_resultat,
        -- todo: legg til k_vedtak_s for å kunne skille ut FERDIG + IVERKS/AVBR etter snapshot
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
        kravhode_id_for -- kh
    from behandling_ferdig
    union all
    select
        sak_id, -- kh
        kravhode_id, -- kh
        'placeholder' as behandling_resultat,
        -- todo: legg til k_vedtak_s for å kunne skille ut FERDIG + IVERKS/AVBR etter snapshot
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
        dato_endret as ferdigbehandlet_tid, --kh
        kravhode_id_for -- kh
    from behandling_avbrutt
    union all
    select
        sak_id, -- kh
        kravhode_id, -- kh
        null as behandling_resultat,
        -- todo: legg til k_vedtak_s for å kunne skille ut FERDIG + IVERKS/AVBR etter snapshot
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
        kravhode_id_for -- kh
    from behandling_andre
)

select
    sak_id, -- kh
    kravhode_id, -- kh
    behandling_resultat,
    -- todo: legg til k_vedtak_s for å kunne skille ut FERDIG + IVERKS/AVBR etter snapshot
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
    cast(systimestamp at time zone 'UTC' as timestamp(9)) as kjoretidspunkt -- brukes til last fra Oracle til BQ
from union_behandling
