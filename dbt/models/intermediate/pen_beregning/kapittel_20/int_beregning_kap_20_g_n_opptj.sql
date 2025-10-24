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
    where
        k_regelverk_t = 'N_REG_G_N_OPPTJ'
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
        k_bor_med_t,
        ber_res_ap_2011_2016_id,
        ber_res_ap_2025_2016_id
    from {{ ref('stg_t_beregning_res') }}
),

ref_beregning_info as (
    select
        beregning_info_id,
        mottar_min_pensjonsniva,
        mottar_min_pensjniva_arsak,
        tt_anv,
        yrksk_anv,
        yrksk_grad,
        yrksk_reg,
        gjenlevrett_anv,
        rett_pa_gjlevenderett,
        inst_opph_anv,
        k_bereg_metode_t,
        tp_restpensjon,
        pt_restpensjon,
        gp_restpensjon
    from {{ ref('stg_t_beregning_info') }}
),

ref_beholdning as (
    select
        beregning_info_id,
        k_beholdning_t,
        totalbelop
    from {{ ref('stg_t_beholdning') }}
),

join_beregning_res as (
    select
        ref_int_lopende_vedtak_alder.*,
        br.beregning_res_id,
        br.ber_res_ap_2011_2016_id,
        br.ber_res_ap_2025_2016_id,
        br.k_bor_med_t,
        br.pen_under_utbet_id,
        br.beregning_info_id
    from ref_int_lopende_vedtak_alder
    inner join ref_beregning_res br
        on
            ref_int_lopende_vedtak_alder.vedtak_id = br.vedtak_id
            and br.dato_virk_fom <= {{ periode_sluttdato(var("periode")) }}
            and (br.dato_virk_tom is null or br.dato_virk_tom >= trunc({{ periode_sluttdato(var("periode")) }}))
),

-- 2016<->2025 er overgangskull
-- Nytt regelverk med gammel OG ny opptjening (N_REG_G_N_OPPTJ) 
join_beregning_info_overgang_2016 as (
    select
        -- beregning_info_id alltid null for disse radene
        join_beregning_res.*,
        -- beregning_info gammel opptjening er kilde for inst_opph, mottar_min_pensjonsniva, gjenlevrett_anv, yrksk_anv og yrksk_grad
        -- i beregning_info ny opptjening er ikke feltene satt (unntak tt_anv)
        bi_2025.beregning_info_id as beregning_info_id_2025, -- brukes i neste cte for å hente beholdning
        bi_2011.tt_anv as tt_anv_g_opptj,
        bi_2011.mottar_min_pensjonsniva, -- dekker også 2025_2016 opptjening (21 rader)
        bi_2011.mottar_min_pensjniva_arsak, -- ser lik ut for 2011 og 2025
        bi_2011.inst_opph_anv, -- dekker også 2025_2016 opptjening (1 rad)
        bi_2011.gjenlevrett_anv,
        bi_2011.rett_pa_gjlevenderett,
        bi_2011.yrksk_anv,
        bi_2011.yrksk_grad,
        bi_2011.yrksk_reg,
        bi_2011.k_bereg_metode_t, -- TODO: avklare om bi_2025 ogsså skal brukes. Totalt 300 rader med forskjellige verdier
        bi_2011.tp_restpensjon, -- restpensjon gjelder kun nytt regelverk med gammel opptjening 
        bi_2011.pt_restpensjon, -- restpensjon gjelder kun nytt regelverk med gammel opptjening
        bi_2011.gp_restpensjon, -- restpensjon gjelder kun nytt regelverk med gammel opptjening

        bi_avdod.yrksk_reg as yrksk_reg_avdod,
        bi_avdod.yrksk_anv as yrksk_anv_avdod,
        bi_2025.tt_anv as tt_anv_n_opptj
    from
        join_beregning_res
    -- hent beregning info kap 19
    left join ref_beregning_res br_2011 on join_beregning_res.beregning_res_id = br_2011.ber_res_ap_2011_2016_id
    left join ref_beregning_info bi_2011 on br_2011.beregning_info_id = bi_2011.beregning_info_id

    -- hent beregning info kap 20
    left join ref_beregning_res br_2025 on join_beregning_res.beregning_res_id = br_2025.ber_res_ap_2025_2016_id
    left join ref_beregning_info bi_2025 on br_2025.beregning_info_id = bi_2025.beregning_info_id

    -- hent beregning_info avdod
    left join ref_beregning_info bi_avdod on br_2011.beregning_info_avdod = bi_avdod.beregning_info_id
),

-- Legg til beholdninginfo, kun for N_REG_N_OPPTJ
-- OBS - bruker beregning_info_id_2025 (nytt regelverk)
join_beholdning as (
    select
        join_beregning_info_overgang_2016.*,
        beh_pen_b.totalbelop as beh_pen_b_totalbelop,
        beh_gar_pen_b.totalbelop as beh_gar_pen_b_totalbelop,
        beh_gar_t_b.totalbelop as beh_gar_t_b_totalbelop
    from join_beregning_info_overgang_2016
    left join ref_beholdning beh_pen_b on join_beregning_info_overgang_2016.beregning_info_id_2025 = beh_pen_b.beregning_info_id and beh_pen_b.k_beholdning_t = 'PEN_B'
    left join ref_beholdning beh_gar_pen_b on join_beregning_info_overgang_2016.beregning_info_id_2025 = beh_gar_pen_b.beregning_info_id and beh_gar_pen_b.k_beholdning_t = 'GAR_PEN_B'
    left join ref_beholdning beh_gar_t_b on join_beregning_info_overgang_2016.beregning_info_id_2025 = beh_gar_t_b.beregning_info_id and beh_gar_t_b.k_beholdning_t = 'GAR_T_B'
)

select
    vedtak_id,
    sak_id,
    kravhode_id,
    k_sak_t,
    dato_lopende_fom,
    dato_lopende_tom,
    k_regelverk_t,
    beregning_res_id,
    pen_under_utbet_id,
    beregning_info_id,
    k_bor_med_t,
    mottar_min_pensjonsniva,
    mottar_min_pensjniva_arsak,
    inst_opph_anv,
    gjenlevrett_anv,
    rett_pa_gjlevenderett,
    yrksk_anv,
    yrksk_grad,
    yrksk_reg,
    yrksk_reg_avdod,
    yrksk_anv_avdod,
    tt_anv_g_opptj,
    tt_anv_n_opptj,
    k_bereg_metode_t,
    tp_restpensjon,
    pt_restpensjon,
    gp_restpensjon,
    beh_pen_b_totalbelop,
    beh_gar_pen_b_totalbelop,
    beh_gar_t_b_totalbelop
from join_beholdning
