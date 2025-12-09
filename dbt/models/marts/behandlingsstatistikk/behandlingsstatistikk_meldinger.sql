-- behandlingsstatistikk_meldinger
-- view som mapper over kolonnenavn fra pen til det team sak ønsker

with

ref_int_behandling as (
    select
        sak_id, -- kh
        kravhode_id, -- kh
        vedtak_id, -- v
        k_krav_gjelder, -- kh
        k_krav_s, -- kh
        k_sak_t, -- v
        k_vilkar_resul_t, -- v behandlingResultat
        k_klageank_res_t, -- v behandlingResultat
        k_vedtak_s, -- v behandlingResultat
        k_krav_arsak_t, -- ka
        k_vedtak_t, -- v
        k_behandling_t, -- kh
        opprettet_av, -- kh
        dato_opprettet, -- kh
        dato_vedtak, -- v
        dato_virk_fom, -- v
        dato_onsket_virk, -- kh
        dato_mottatt_krav, -- kh
        kravhode_id_for -- kh
    from {{ ref('int_behandling') }}
),

final as (
    select
        sak_id,
        kravhode_id as b_id,
        vedtak_id, -- bonuskolonne
        k_krav_arsak_t as b_arsak,
        k_krav_s as b_status,
        k_krav_gjelder as b_type,
        k_vilkar_resul_t as b_resultat_vilkar_resul_t,
        k_klageank_res_t as b_resultat_klageank_res_t,
        k_vedtak_s as b_resultat_vedtak_s,
        k_behandling_t as b_metode,
        k_sak_t as sak_ytelse,
        dato_mottatt_krav as mottatt_tid,
        dato_opprettet as registrert_tid,
        dato_onsket_virk as forventetoppstart_tid,
        opprettet_av,
        dato_virk_fom as utbetalt_tid,
        kravhode_id_for as relatertbehandling_id,
        'bonuskolonner til høyre' as bonus,
        k_vedtak_t
    from ref_int_behandling
)

select * from final
