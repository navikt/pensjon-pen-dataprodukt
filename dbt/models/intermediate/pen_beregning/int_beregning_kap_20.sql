with

kap_20_g_n_opptj as (
    select * from {{ ref('int_beregning_kap_20_g_n_opptj') }}
),

kap_20_n_opptj as (
    select * from {{ ref('int_beregning_kap_20_n_opptj') }}
),

kap_20_g_opptj as (
    select * from {{ ref('int_beregning_kap_20_g_opptj') }}
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

union_beregning_info as (
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
    from kap_20_g_n_opptj
    union all
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
        null as tt_anv_g_opptj,
        tt_anv_n_opptj,
        k_bereg_metode_t,
        0 as tp_restpensjon,
        0 as pt_restpensjon,
        0 as gp_restpensjon,
        beh_pen_b_totalbelop,
        beh_gar_pen_b_totalbelop,
        beh_gar_t_b_totalbelop
    from kap_20_n_opptj
    union all
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
        null as tt_anv_n_opptj,
        k_bereg_metode_t,
        tp_restpensjon,
        pt_restpensjon,
        gp_restpensjon,
        null as beh_pen_b_totalbelop,
        null as beh_gar_pen_b_totalbelop,
        null as beh_gar_t_b_totalbelop
    from kap_20_g_opptj
),

join_uttaksgrad as (
    select
        union_beregning_info.*,
        ref_uttaksgrad.uttaksgrad
    from union_beregning_info
    inner join ref_uttaksgrad
        on
            union_beregning_info.kravhode_id = ref_uttaksgrad.kravhode_id
            and ref_uttaksgrad.dato_virk_fom <= {{ periode_sluttdato(var("periode")) }}
            and (ref_uttaksgrad.dato_virk_tom is null or ref_uttaksgrad.dato_virk_tom >= trunc({{ periode_sluttdato(var("periode")) }}))
),

join_pen_under_utbet as (
    -- todo: sjekke om brutto og netto er samme som i ytelse_komp eller beregning_res
    select
        join_uttaksgrad.*,
        ref_pen_under_utbet.total_belop_brutto as brutto,
        ref_pen_under_utbet.total_belop_netto as netto
    from join_uttaksgrad
    inner join ref_pen_under_utbet
        on join_uttaksgrad.pen_under_utbet_id = ref_pen_under_utbet.pen_under_utbet_id
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
    brutto,
    netto,
    tp_restpensjon,
    pt_restpensjon,
    gp_restpensjon,
    beh_pen_b_totalbelop,
    beh_gar_pen_b_totalbelop,
    beh_gar_t_b_totalbelop
from join_pen_under_utbet
