{{
  config(
    materialized = 'table',
    )
}}

select
    sak_id,
    vedtak_id,
    k_regelverk_t,
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
    beh_pen_b_totalbelop,
    beh_gar_pen_b_totalbelop,
    beh_gar_t_b_totalbelop,
    minstepen_niva_arsak,
    institusjon_opphold,
    yrkesskade_anv_flagg,
    yrkesskade_rett_flagg,
    gjenlevrett_anv,
    rett_pa_gjlevenderett,
    minstepensjon,
    alderspensjon_ytelse_flagg,

    {{ var("periode") }} as periode,
    sysdate as kjoretidspunkt
from {{ ref('int_beregning') }}
where
    1 = 1
{% if is_incremental() %}
    and {{ var("periode") }} not in (select distinct periode from {{ this }}) -- noqa
{% endif %}
