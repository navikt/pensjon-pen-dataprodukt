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
    kravhode_id as kh_kravhode_id,
    kravhode_id_for as kh_kravhode_id_for,
    sak_id as kh_sak_id,
    dato_mottatt_krav as kh_dato_mottatt_krav,
    dato_opprettet as kh_dato_opprettet,
    dato_onsket_virk as kh_dato_onsket_virk,
    k_krav_gjelder as kh_k_krav_gjelder,
    k_krav_s as kh_k_krav_s,
    opprettet_av as kh_opprettet_av,
    k_behandling_t as kh_k_behandling_t,
    vedtak_id as v_vedtak_id,
    k_sak_t as v_k_sak_t,
    k_vedtak_t as v_k_vedtak_t,
    dato_vedtak as v_dato_vedtak,
    dato_virk_fom as v_dato_virk_fom,
    k_vilkar_resul_t as v_k_vilkar_resul_t
from behandlinger_vedtak
order by
    kh_sak_id desc,
    kh_kravhode_id desc,
    kh_dato_mottatt_krav desc,
    v_vedtak_id desc
