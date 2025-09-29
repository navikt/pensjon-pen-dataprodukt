with

ref_int_lopende_vedtak_alder as (
    select
        vedtak_id,
        sak_id,
        kravhode_id,
        k_sak_t,
        dato_lopende_fom,
        dato_lopende_tom,
        k_regelverk_t
    from {{ ref('int_lopende_vedtak_alder') }}
),

ref_beregning_res as (
    select
        beregning_res_id,
        vedtak_id,
        dato_virk_fom,
        dato_virk_tom,
        beregning_info_id,
        pen_under_utbet_id,
        beregning_info_avdod,
        ber_res_ap_2011_2016_id,
        ber_res_ap_2025_2016_id
    from {{ ref('stg_t_beregning_res') }}
),

ref_uttaksgrad as (
    select
        kravhode_id,
        dato_virk_fom,
        dato_virk_tom,
        uttaksgrad
    from {{ ref('stg_t_uttaksgrad') }}
),

ref_pen_under_utbet as (
    select
        pen_under_utbet_id,
        total_belop_brutto,
        total_belop_netto
    from {{ ref('stg_t_pen_under_utbet') }}
),

ref_beregning_info as (
    select
        beregning_info_id,
        mottar_min_pensjonsniva,
        tt_anv,
        yrksk_anv,
        yrksk_grad,
        gjenlevrett_anv,
        inst_opph_anv
    from {{ ref('stg_t_beregning_info') }}
),

join_uttaksgrad as (
    select
        ref_int_lopende_vedtak_alder.*,
        ref_uttaksgrad.uttaksgrad
    from ref_int_lopende_vedtak_alder
    inner join ref_uttaksgrad
        on
            ref_int_lopende_vedtak_alder.kravhode_id = ref_uttaksgrad.kravhode_id
            and ref_uttaksgrad.uttaksgrad != 0
            and ref_uttaksgrad.dato_virk_fom <= {{ periode_sluttdato(var("periode")) }}
            and (ref_uttaksgrad.dato_virk_tom is null or ref_uttaksgrad.dato_virk_tom >= trunc({{ periode_sluttdato(var("periode")) }}))
),

join_beregning_res as (
    select
        join_uttaksgrad.*,
        br.beregning_res_id,
        br.ber_res_ap_2011_2016_id,
        br.ber_res_ap_2025_2016_id,
        br.pen_under_utbet_id,
        br.beregning_info_id
    from join_uttaksgrad
    inner join ref_beregning_res br
        on
            join_uttaksgrad.vedtak_id = br.vedtak_id
            and br.dato_virk_fom <= {{ periode_sluttdato(var("periode")) }}
            and (br.dato_virk_tom is null or br.dato_virk_tom >= trunc({{ periode_sluttdato(var("periode")) }}))
),

-- 2025-> Nytt regelverk med NY opptjening (N_REG_N_OPPTJ)
-- ELLER
-- 2011->2016 Nytt regelverk med GAMMEL opptjening (N_REG_G_OPPTJ) 
join_beregning_info_direkte as (
    select
        join_beregning_res.*,
        ref_beregning_info.mottar_min_pensjonsniva,
        ref_beregning_info.inst_opph_anv,
        ref_beregning_info.gjenlevrett_anv,
        ref_beregning_info.yrksk_anv,
        ref_beregning_info.yrksk_grad,
        case when join_beregning_res.k_regelverk_t = 'N_REG_G_OPPTJ' then ref_beregning_info.tt_anv end as tt_anv_g_opptj,
        case when join_beregning_res.k_regelverk_t = 'N_REG_N_OPPTJ' then ref_beregning_info.tt_anv end as tt_anv_n_opptj
    from join_beregning_res
    inner join ref_beregning_info
        on join_beregning_res.beregning_info_id = ref_beregning_info.beregning_info_id
    where join_beregning_res.k_regelverk_t in ('N_REG_G_OPPTJ', 'N_REG_N_OPPTJ') -- 2011 og 2025
),

-- 2016<->2025 er overgangskull
-- Nytt regelverk med gammel OG ny opptjening (N_REG_G_N_OPPTJ) 
join_beregning_info_overgang_2016 as (
    select
        -- beregning_info_id alltid null for disse radene
        join_beregning_res.*,
        -- beregning_info gammel opptjening er kilde for inst_opph, mottar_min_pensjonsniva, gjenlevrett_anv, yrksk_anv og yrksk_grad
        -- i beregning_info ny opptjening er ikke feltene satt (unntak tt_anv)
        bi_2011.tt_anv as tt_anv_g_opptj,
        bi_2011.mottar_min_pensjonsniva, -- dekker også 2025_2016 opptjening (21 rader)
        bi_2011.inst_opph_anv, -- dekker også 2025_2016 opptjening (1 rad)
        bi_2011.gjenlevrett_anv,
        bi_2011.yrksk_anv,
        bi_2011.yrksk_grad,

        bi_2025.tt_anv as tt_anv_n_opptj
    from
        join_beregning_res
    -- hent beregning info kap 19
    left join ref_beregning_res br_2011 on join_beregning_res.beregning_res_id = br_2011.ber_res_ap_2011_2016_id
    left join ref_beregning_info bi_2011 on br_2011.beregning_info_id = bi_2011.beregning_info_id

    -- hent beregning info kap 20
    left join ref_beregning_res br_2025 on join_beregning_res.beregning_res_id = br_2025.ber_res_ap_2025_2016_id
    left join ref_beregning_info bi_2025 on br_2025.beregning_info_id = bi_2025.beregning_info_id
    where
        join_beregning_res.k_regelverk_t = 'N_REG_G_N_OPPTJ'
),

union_beregning_info as (
    select
        vedtak_id,
        sak_id,
        kravhode_id,
        k_sak_t,
        dato_lopende_fom,
        dato_lopende_tom,
        k_regelverk_t,
        uttaksgrad,
        beregning_res_id,
        pen_under_utbet_id,
        beregning_info_id,
        mottar_min_pensjonsniva,
        inst_opph_anv,
        gjenlevrett_anv,
        yrksk_anv,
        yrksk_grad,
        tt_anv_g_opptj,
        tt_anv_n_opptj
    from join_beregning_info_direkte
    union all
    select
        vedtak_id,
        sak_id,
        kravhode_id,
        k_sak_t,
        dato_lopende_fom,
        dato_lopende_tom,
        k_regelverk_t,
        uttaksgrad,
        beregning_res_id,
        pen_under_utbet_id,
        beregning_info_id,
        mottar_min_pensjonsniva,
        inst_opph_anv,
        gjenlevrett_anv,
        yrksk_anv,
        yrksk_grad,
        tt_anv_g_opptj,
        tt_anv_n_opptj
    from join_beregning_info_overgang_2016
),

join_pen_under_utbet as (
    -- todo: sjekke om brutto og netto er samme som i ytelse_komp eller beregning_res
    select
        union_beregning_info.*,
        ref_pen_under_utbet.total_belop_brutto as brutto,
        ref_pen_under_utbet.total_belop_netto as netto
    from union_beregning_info
    inner join ref_pen_under_utbet
        on union_beregning_info.pen_under_utbet_id = ref_pen_under_utbet.pen_under_utbet_id
)

select
    vedtak_id,
    sak_id,
    kravhode_id,
    k_sak_t,
    dato_lopende_fom,
    dato_lopende_tom,
    k_regelverk_t,
    uttaksgrad,
    beregning_res_id,
    pen_under_utbet_id,
    beregning_info_id,
    mottar_min_pensjonsniva,
    inst_opph_anv,
    gjenlevrett_anv,
    yrksk_anv,
    yrksk_grad,
    tt_anv_g_opptj,
    tt_anv_n_opptj,
    brutto,
    netto
from join_pen_under_utbet
