-- wendelboe_pesys_alder_I_current

with

ref_int_beregning as (
    select * from {{ ref('dataprodukt_alderspensjon_beregning') }}
),

ref_int_beregning_belop as (
    select * from {{ ref('dataprodukt_alderspensjon_belop') }}
),

ref_int_vedtaksinfo as (
    select * from {{ ref('dataprodukt_alderspensjon_vedtak') }}
),

dataprodukt_1 as (
    select
        v.periode,
        v.vedtak_id,
        v.sak_id,
        v.kravhode_id,
        v.k_vedtak_t,
        v.k_vedtak_s,
        null as persnr,
        v.person_id,
        v.dato_lopende_fom,
        v.dato_lopende_tom,
        v.k_afp_t as afp_ordning,
        v.afp_privat_flagg,
        v.k_regelverk_t as regelverk,
        v.k_sak_t as sakstype,
        b.uttaksgrad,
        b.netto,
        b.alderspensjon_ytelse_flagg as aldersytelseflagg,
        b.minstepensjon,
        bb.k_minstepen_niva as minste_pen_niva,
        v.overgangsstonad_flagg as overgangsstonad,
        bb.gp_netto,
        bb.tp_netto,
        bb.pt_netto,
        bb.st_netto,
        bb.et_netto,
        bb.saerkull_netto,
        bb.barn_felles_netto,
        bb.mpn_sstot_netto,
        bb.mpn_indiv_netto,
        bb.skjermt_netto,
        bb.ufor_sum_ut_ord_netto,
        bb.gjt_netto,
        bb.gjt_k19_netto,
        bb.ap_kap19_med_gjr_bel,
        bb.ap_kap19_uten_gjr_bel,
        bb.ip_netto,
        bb.gap_netto,
        -- b.anvendt_yrkesskade_flagg as yrkesskade_rett_flagg, -- TODO
        -- yrkesskade_anv_flagg, -- TODO
        b.institusjon_opphold as red_pga_inst_opph_flagg,
        b.tt_anv_g_opptj as tt_anvendt_kap19_antall,
        b.tt_anv_n_opptj as tt_anvendt_kap20_antall,
        b.rett_pa_gjlevenderett as innv_gj_rett -- TODO 
        -- kommunal_ytelse,
        -- kjoretidspunkt

    from ref_int_vedtaksinfo v
    inner join ref_int_beregning b
        on
            v.vedtak_id = b.vedtak_id
            and v.periode = b.periode
    inner join ref_int_beregning_belop bb
        on
            v.vedtak_id = bb.vedtak_id
            and v.periode = bb.periode
)

select * from dataprodukt_1
