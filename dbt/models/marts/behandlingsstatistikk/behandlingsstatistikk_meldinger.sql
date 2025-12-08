-- behandlingsstatistikk_meldinger
-- view som mapper over kolonnenavn fra pen til det team sak ønsker

with

ref_int_behandling as (
    select
        kravhode_id, -- kh
        kravhode_id_for, -- kh
        sak_id, -- kh
        dato_mottatt_krav, -- kh
        dato_opprettet, -- kh
        dato_onsket_virk, -- kh
        k_krav_gjelder, -- kh
        k_krav_s, -- kh
        opprettet_av, -- kh
        k_behandling_t, -- kh
        vedtak_id, -- v
        k_sak_t, -- v
        k_vedtak_t, -- v
        dato_vedtak, -- v
        dato_virk_fom, -- v
        k_vilkar_resul_t -- v
    from {{ ref('int_behandling') }}
),

final as (
    select
        kravhode_id as behandling_id,
        kravhode_id_for as relatertbehandling_id,
        sak_id,
        dato_mottatt_krav as mottatt_tid,
        dato_opprettet as registrert_tid,
        dato_onsket_virk as forventetoppstart_tid,
        k_krav_gjelder as behandling_type,
        k_krav_s as behandling_status,
        opprettet_av,
        k_behandling_t as metode,
        k_sak_t as sak_ytelse,
        k_vedtak_t,
        dato_virk_fom as utbetalt_tid,
        k_vilkar_resul_t as behandling_resultat
    from ref_int_behandling
)

select * from final
