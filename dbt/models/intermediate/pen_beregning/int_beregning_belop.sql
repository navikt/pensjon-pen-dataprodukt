-- int_beregning_belop
-- henter beløp for beregninger ved å pakke ut ytelse_komp, både for kap19 og kap20

with

ref_int_beregning as (
    select
        vedtak_id,
        sak_id,
        beregning_id,
        pen_under_utbet_id,
        brutto,
        netto
    from {{ ref('int_beregning') }}
),

ref_ytelse_komp as (
    select
        beregning_id,
        pen_under_utbet_id,
        k_ytelse_komp_t,
        ap_kap19_med_gjr,
        ap_kap19_uten_gjr,
        k_minstepen_niva,
        min_pen_niva_id,
        anvendt_trygdetid,
        netto,
        opphort,
        fradrag,
        bruk
    from {{ ref('stg_t_ytelse_komp') }}
),

ref_min_pen_niva as (
    select
        min_pen_niva_id,
        sats
    from {{ ref('stg_t_min_pen_niva') }}
),

ref_stg_t_anvendt_trygdetid as (
    select
        anvendt_trygdetid_id,
        pro_rata
    from {{ ref('stg_t_anvendt_trygdetid') }}
),

ref_stg_t_brok as (
    select
        brok_id,
        teller,
        nevner
    from {{ ref('stg_t_brok') }}
),

transponert_ytelse_komp as (
    -- selv om det er sum per type ytelse, så er det kun en rad per beregning_id eller pen_under_utbet_id
    select
        beregning_id,
        pen_under_utbet_id,
        sum(case when k_ytelse_komp_t = 'GP' and netto > 0 and fradrag > 0 then 1 else 0 end) as gp_avkorting_flagg, -- todo: sjekk output
        sum(fradrag) as sum_fradrag, -- todo: denne er alltid 0!!!
        max(case when k_ytelse_komp_t = 'PT' then k_minstepen_niva end) as k_minstepen_niva,
        max(case when k_ytelse_komp_t = 'MIN_NIVA_TILL_INDV' then min_pen_niva_id end) as min_pen_niva_id,
        max(case when k_ytelse_komp_t = 'GP' then anvendt_trygdetid end) as yk_anvendt_trygdetid, -- id for å joine t_anvendt_trygdetid -> t_brok -> pro_rata

        sum(case when k_ytelse_komp_t = 'GP' then netto end) as gp_netto,
        sum(case when k_ytelse_komp_t = 'TP' then netto end) as tp_netto,
        sum(case when k_ytelse_komp_t = 'PT' then netto end) as pt_netto,
        sum(case when k_ytelse_komp_t = 'ST' then netto end) as st_netto,
        sum(case when k_ytelse_komp_t = 'IP' then netto end) as ip_netto,
        sum(case when k_ytelse_komp_t = 'ET' then netto end) as et_netto,
        sum(case when k_ytelse_komp_t = 'VT' then netto end) as vt_netto,
        sum(case when k_ytelse_komp_t = 'GAP' then netto end) as gap_netto,
        sum(case when k_ytelse_komp_t = 'GAT' then netto end) as gat_netto,
        sum(case when k_ytelse_komp_t = 'AP_GJT' then netto end) as gjt_netto,
        sum(case when k_ytelse_komp_t = 'AFP_T' then netto end) as afp_t_netto,
        sum(case when k_ytelse_komp_t = 'TSB' then netto end) as saerkull_netto,
        sum(case when k_ytelse_komp_t = 'TFB' then netto end) as barn_felles_netto,
        sum(case when k_ytelse_komp_t = 'SKJERMT' then netto end) as skjermt_netto,
        sum(case when k_ytelse_komp_t = 'AP_GJT_KAP19' then netto end) as gjt_k19_netto,
        sum(case when k_ytelse_komp_t = 'UT_ORDINER' then netto end) as ufor_sum_ut_ord_netto,
        sum(case when k_ytelse_komp_t = 'AFP_LIVSVARIG' then netto end) as afp_livsvarig_netto,
        sum(case when k_ytelse_komp_t = 'MIN_NIVA_TILL_INDV' then netto end) as mpn_indiv_netto,
        sum(case when k_ytelse_komp_t = 'MIN_NIVA_TILL_PPAR' then netto end) as mpn_sstot_netto,
        sum(case when k_ytelse_komp_t = 'AP_GJT_KAP19' then ap_kap19_med_gjr end) as ap_kap19_med_gjr_bel,
        sum(case when k_ytelse_komp_t = 'AP_GJT_KAP19' then ap_kap19_uten_gjr end) as ap_kap19_uten_gjr_bel
    from ref_ytelse_komp
    where
        bruk = '1'
        and opphort = '0'
    group by
        beregning_id,
        pen_under_utbet_id -- en rad per type ytelse, enten kap19 eller kap20
),

join_beregning as (
    select
        ref_int_beregning.*,
        transponert_ytelse_komp.gp_avkorting_flagg,
        transponert_ytelse_komp.sum_fradrag,
        transponert_ytelse_komp.k_minstepen_niva,
        transponert_ytelse_komp.min_pen_niva_id,
        transponert_ytelse_komp.yk_anvendt_trygdetid,
        transponert_ytelse_komp.gp_netto,
        transponert_ytelse_komp.tp_netto,
        transponert_ytelse_komp.pt_netto,
        transponert_ytelse_komp.st_netto,
        transponert_ytelse_komp.ip_netto,
        transponert_ytelse_komp.et_netto,
        transponert_ytelse_komp.vt_netto,
        transponert_ytelse_komp.gap_netto,
        transponert_ytelse_komp.gat_netto,
        transponert_ytelse_komp.gjt_netto,
        transponert_ytelse_komp.afp_t_netto,
        transponert_ytelse_komp.saerkull_netto,
        transponert_ytelse_komp.barn_felles_netto,
        transponert_ytelse_komp.skjermt_netto,
        transponert_ytelse_komp.gjt_k19_netto,
        transponert_ytelse_komp.ufor_sum_ut_ord_netto,
        transponert_ytelse_komp.afp_livsvarig_netto,
        transponert_ytelse_komp.mpn_indiv_netto,
        transponert_ytelse_komp.mpn_sstot_netto,
        transponert_ytelse_komp.ap_kap19_med_gjr_bel,
        transponert_ytelse_komp.ap_kap19_uten_gjr_bel
    from ref_int_beregning
    left join transponert_ytelse_komp
        on
            ref_int_beregning.pen_under_utbet_id = transponert_ytelse_komp.pen_under_utbet_id
            or ref_int_beregning.beregning_id = transponert_ytelse_komp.beregning_id
),

legger_til_minstepen_niva_sats as (
    select
        join_beregning.*,
        ref_min_pen_niva.sats as minstepen_niva_sats
    from join_beregning
    left join ref_min_pen_niva
        on join_beregning.min_pen_niva_id = ref_min_pen_niva.min_pen_niva_id
),

-- Prorata kan også hentes fra:
-- yk.f_min_pen_niva_id -> t_f_min_pen_niva_id -> t_min_pen_niva.pro_rata_teller/nevner_mnd
legger_til_prorata as (
    select
        v.*,
        brok.teller as prorata_teller,
        brok.nevner as prorata_nevner
    from legger_til_minstepen_niva_sats v
    left join ref_stg_t_anvendt_trygdetid tat on v.yk_anvendt_trygdetid = tat.anvendt_trygdetid_id
    left join ref_stg_t_brok brok on tat.pro_rata = brok.brok_id
)

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
    gp_avkorting_flagg
from legger_til_prorata
