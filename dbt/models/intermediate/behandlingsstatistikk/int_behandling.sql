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
    -- from pen.t_kravhode kh
    from {{ ref('stg_t_kravhode') }} kh
    order by
        kh.sak_id desc,
        kh.dato_mottatt_krav desc
),

behandlinger_kravarsak as (
    -- en kravarsak per kravhode
    select
        beh.*,
        ka.k_krav_arsak_t -- behandlingAarsak*
    from behandlinger_kravhode beh
    -- left join pen.t_krav_arsak ka
    left join {{ ref('stg_t_krav_arsak') }} ka
        on beh.kravhode_id = ka.kravhode_id
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
        v.k_vedtak_s, -- mulig deler av behandlingResultat (feks AVBR, men kan også være fra k_krav_s)
        v.k_klageank_res_t, -- deler av behandlingResultat
        v.k_vilkar_resul_t -- deler av behandlingResultat
    from behandlinger_kravarsak beh
    -- left join pen.t_vedtak v
    left join {{ ref('stg_t_vedtak') }} v
        on beh.kravhode_id = v.kravhode_id
    order by beh.sak_id desc, v.dato_vedtak desc
)

select
    sak_id, -- kh
    kravhode_id, -- kh
    vedtak_id, -- v
    k_krav_gjelder, -- kh
    k_krav_s, -- kh
    k_sak_t, -- v
    k_vilkar_resul_t, -- v
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
from behandlinger_vedtak
order by
    sak_id desc,
    kravhode_id desc,
    dato_mottatt_krav desc,
    vedtak_id desc
