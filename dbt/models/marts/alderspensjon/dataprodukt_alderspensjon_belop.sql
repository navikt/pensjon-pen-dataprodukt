{{
  config(
    materialized = 'incremental',
    )
}}

select
    vedtak_id,
    sak_id,
    beregning_id,
    pen_under_utbet_id,
    brutto,
    netto,

    sum_fradrag, -- obs! denne er alltid 0, også i prod
    k_minstepen_niva,
    minstepen_niva_sats,
    prorata_teller,
    prorata_nevner,
    psats_gp,

    gp_netto,
    tp_netto,
    pt_netto,
    st_netto,
    ip_netto,
    et_netto,
    vt_netto,
    gap_netto,
    gat_netto,
    gjt_netto,
    afp_t_netto,
    saerkull_netto,
    barn_felles_netto,
    skjermt_netto,
    gjt_k19_netto,
    ufor_sum_ut_ord_netto,
    afp_livsvarig_netto,
    mpn_indiv_netto,
    mpn_sstot_netto,

    ap_kap19_med_gjr_bel,
    ap_kap19_uten_gjr_bel,
    {{ var("periode") }} as periode,
    sysdate as kjoretidspunkt
from {{ ref('int_beregning_belop') }}
where
    1 = 1
{% if is_incremental() %}
    and {{ var("periode") }} not in (select distinct periode from {{ this }}) -- noqa
{% endif %}
