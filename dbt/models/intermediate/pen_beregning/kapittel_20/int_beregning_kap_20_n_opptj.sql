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
    where k_regelverk_t = 'N_REG_N_OPPTJ'
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

join_beregning_res as (
    select
        ref_int_lopende_vedtak_alder.*,
        br.beregning_res_id,
        br.ber_res_ap_2011_2016_id,
        br.ber_res_ap_2025_2016_id,
        br.k_bor_med_t,
        br.pen_under_utbet_id,
        br.beregning_info_id,
        br.beregning_info_avdod
    from ref_int_lopende_vedtak_alder
    inner join ref_beregning_res br
        on
            ref_int_lopende_vedtak_alder.vedtak_id = br.vedtak_id
            and br.dato_virk_fom <= {{ periode_sluttdato(var("periode")) }}
            and (br.dato_virk_tom is null or br.dato_virk_tom >= trunc({{ periode_sluttdato(var("periode")) }}))
),

-- Join beregning_info for NY OPPTJENING
-- TT_ANV settes til TT_ANV_N_OPPTJ
-- nytt regelverk har ikke restpensjon
join_beregning_info as (
    select
        join_beregning_res.*,
        ref_beregning_info.mottar_min_pensjonsniva,
        ref_beregning_info.mottar_min_pensjniva_arsak,
        ref_beregning_info.inst_opph_anv,
        ref_beregning_info.gjenlevrett_anv,
        ref_beregning_info.rett_pa_gjlevenderett,
        ref_beregning_info.yrksk_anv,
        ref_beregning_info.yrksk_grad,
        ref_beregning_info.yrksk_reg,
        ref_beregning_info.k_bereg_metode_t,
        ref_beregning_info.tt_anv as tt_anv_n_opptj,
        bi_avdod.yrksk_reg as yrksk_reg_avdod,
        bi_avdod.yrksk_anv as yrksk_anv_avdod
    from join_beregning_res
    inner join ref_beregning_info
        on join_beregning_res.beregning_info_id = ref_beregning_info.beregning_info_id
    left join ref_beregning_info bi_avdod
        on join_beregning_res.beregning_info_avdod = bi_avdod.beregning_info_id
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
    tt_anv_n_opptj,
    k_bereg_metode_t
from join_beregning_info
