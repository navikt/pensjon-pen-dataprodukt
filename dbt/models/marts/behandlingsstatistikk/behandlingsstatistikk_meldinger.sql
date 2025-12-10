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
        k_utlandstilknytning, -- sak
        k_sak_s, -- sak
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
        kravhode_id as behandling_id,
        kravhode_id_for as relatertbehandling_id,
        'PESYS' as relatert_fagsystem,
        sak_id,
        '-1' as saksnummer,
        '-1' as aktor_id,
        dato_mottatt_krav as mottatt_tid,
        dato_opprettet as registrert_tid,
        null as ferdigbehandlet_tid,
        dato_virk_fom as utbetalt_tid,
        null as funksjonell_tid,
        dato_onsket_virk as forventetoppstart_tid,
        null as teknisk_tid,
        k_sak_t as sak_ytelse,
        k_utlandstilknytning as sak_utland,
        k_krav_gjelder as behandling_type,
        k_krav_s as behandling_status,
        k_vilkar_resul_t as behandling_resultat_vilkar_resul_t,
        k_klageank_res_t as behandling_resultat_klageank_res_t,
        k_vedtak_s as behandling_resultat_vedtak_s,
        k_sak_s as behandling_resultat_sak_s,
        '-1' as resultat_begrunnelse,
        k_behandling_t as behandling_metode,
        k_krav_arsak_t as behandling_arsak,
        opprettet_av,
        '-1' as saksbehandler,
        '-1' as ansvarlig_beslutter,
        '-1' as ansvarlig_enhet,
        '-1' as tilbakekrev_belop,
        '-1' as funksjonell_periode_fom,
        '-1' as funksjonell_periode_tom,
        'PESYS' as fagsystem_navn,
        '-1' as fagsystem_versjon,
        'bonuskolonner til høyre' as bonus,
        vedtak_id, -- bonuskolonne
        k_vedtak_t
    from ref_int_behandling
)

select * from final
