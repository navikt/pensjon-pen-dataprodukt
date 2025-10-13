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
        ber_res_ap_2011_2016_id,
        ber_res_ap_2025_2016_id
    from {{ ref('stg_t_beregning_res') }}
),

ref_beregning_info as (
    select
        beregning_info_id,
        mottar_min_pensjonsniva,
        tt_anv,
        yrksk_anv,
        yrksk_grad,
        gjenlevrett_anv,
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
        br.pen_under_utbet_id,
        br.beregning_info_id
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
        ref_beregning_info.inst_opph_anv,
        ref_beregning_info.gjenlevrett_anv,
        ref_beregning_info.yrksk_anv,
        ref_beregning_info.yrksk_grad,
        ref_beregning_info.k_bereg_metode_t,
        ref_beregning_info.tt_anv as tt_anv_n_opptj
    from join_beregning_res
    inner join ref_beregning_info
        on join_beregning_res.beregning_info_id = ref_beregning_info.beregning_info_id
)

-- -- Legg til beholdninginfo, kun for N_REG_N_OPPTJ
-- join_beholdning as (
--     select 
--         join_beregning_info.*,
--         b.* -- TOTO aggreger k_beholdning_t
--     from join_beregning_info
--     left join ref_beholdning b on b.beregning_info_id = join_beregning_info.beregning_info_id
-- )

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
    mottar_min_pensjonsniva,
    inst_opph_anv,
    gjenlevrett_anv,
    yrksk_anv,
    yrksk_grad,
    --tt_anv_g_opptj,
    tt_anv_n_opptj,
    k_bereg_metode_t
    -- tp_restpensjon,
    -- pt_restpensjon,
    -- gp_restpensjon
from join_beregning_info
