-- int_behandling

with

behandlinger_kravhode as (
-- en sak kan ha flere behandlinger (kravhoder)
    select
        kh.kravhode_id, -- behandlingId
        kh.kravhode_id_for, -- relatertBehandlingId
        kh.sak_id, -- sakId
        kh.dato_mottatt_krav, -- mottattTid
        kh.dato_opprettet, -- registrertTid
        kh.dato_onsket_virk, -- forventetOppstartTid
        kh.k_krav_gjelder, -- behandlingType*
        kh.k_krav_s, -- behandlingStatus*
        kh.opprettet_av, -- opprettetAv**
        kh.k_behandling_t -- metode
        -- kh.endret_av
    from pen.t_kravhode kh
    order by
        kh.sak_id desc,
        kh.dato_mottatt_krav desc
),

behandlinger_vedtak as (
-- en behandling kan ha flere vedtak
    select
        beh.*,
        v.vedtak_id,
        v.k_sak_t, -- sakYtelse
        v.k_vedtak_t,
        v.dato_vedtak,
        v.dato_virk_fom, -- utbetaltTid
        v.k_vilkar_resul_t -- behandlingResultat*, dekker deler av behandlingResultat
    from behandlinger_kravhode beh
    left join pen.t_vedtak v
        on beh.kravhode_id = v.kravhode_id
    order by beh.sak_id desc, v.dato_vedtak desc
)

-- todo:
-- koble på krav_arsak for behandlingAarsak*
-- kolbe på noe mer behandling-resultat info enn k_vilkar_resul_t
-- kan også være fint å mappe om kolonnenavn til det team sak vil ha for å holde tunga rett i munnen

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
from behandlinger_vedtak
order by
    sak_id desc,
    kravhode_id desc,
    dato_mottatt_krav desc,
    vedtak_id desc
