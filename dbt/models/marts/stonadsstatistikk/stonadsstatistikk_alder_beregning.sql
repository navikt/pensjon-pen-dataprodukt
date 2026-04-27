{{
  config(
    materialized = 'incremental',
    )
}}

select
    to_date({{ var("periode") }}, 'YYYYMM') as periode,
    sak_id,
    vedtak_id,
    kravhode_id,
    k_regelverk_t,
    beregning_id,
    pen_under_utbet_id,
    brutto,
    netto,
    uttaksgrad,
    tt_anv_g_opptj,
    tt_anv_n_opptj,
    k_bereg_metode_t,
    k_bor_med_t,
    tp_restpensjon,
    pt_restpensjon,
    gp_restpensjon,
    beh_pen_b_totalbelop as beholdning_pensjon_belop, -- endre namvet upstream
    beh_gar_pen_b_totalbelop as beholdning_garan_pen_belop, -- endre namvet upstream
    beh_gar_t_b_totalbelop as beholdning_garan_tlg_belop, -- endre namvet upstream
    minstepen_niva_arsak,
    red_pga_inst_opph_flagg,
    yrkesskade_anv_flagg,
    yrkesskade_rett_flagg,
    gjenlevrett_anv,
    innv_gj_rett,
    minstepensjon,
    sysdate as kjoretidspunkt
from {{ ref('int_beregning') }}
where
    1 = 1
{% if is_incremental() %}
    and to_date({{ var("periode") }}, 'YYYYMM') not in (select distinct periode from {{ this }}) -- noqa
{% endif %}
