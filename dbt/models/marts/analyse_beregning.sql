{{
  config(
    materialized = 'incremental',
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

    -- flagg
    institusjon_opphold,
    anvendt_yrkesskade_flagg,
    gjenlevrett_anv,
    minstepensjon,

    pen_under_utbet_id,
    beregning_id,

    -- fjern etter analyse, hører hjemme i vedtak
    cast(null as number) as kravhode_id,  -- fjern etter analyse, hører hjemme i vedtak
    cast(null as varchar2(5)) as k_sak_t,  -- fjern etter analyse, hører hjemme i vedtak
    cast(null as date) as dato_lopende_fom,  -- fjern etter analyse, hører hjemme i vedtak
    cast(null as date) as dato_lopende_tom,  -- fjern etter analyse, hører hjemme i vedtak

    {{ var("periode") }} as periode,
    sysdate as kjoretidspunkt
from {{ ref('int_beregning') }}
where
    1 = 1
{% if is_incremental() %}
    and {{ var("periode") }} not in (select distinct periode from {{ this }}) -- noqa
{% endif %}
