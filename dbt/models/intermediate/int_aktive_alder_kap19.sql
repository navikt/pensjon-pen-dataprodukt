-- int_aktive_alder_kap19

-- finner aktive utbetalinger på alder fra kapittel 19
-- bruker da beregning_res og finner utbetalinger fra ytelse_komp

-- beregning_res er for kapittel 19 (AFP_PRIVAT, ALDER, UFOREP)
-- beregning er for kapittel 20 (eller omvendt)
-- Fra 1. februar 2016 utbetales alderspensjon etter kapittel 20 for første gang.
-- Årskullene 1954-1962 skal ha pensjon delvis etter kapittel 19 og 20.


-- fra SQL-piloten:
-- CTE for å trekke ut de aktive vedtakene, basert på Magne Nordås' SQL-er.
-- Denne kjører løpet via t_beregning_res og t_pen_under_utbet, ned mot ytelse_komp.
-- Rader fra ytelse_komp traverseres og transponeres med aggregeringsfunksjoner opp på en rad per vedtak.

with

ref_vedtak as (
    -- trenger info om vedtaket for alderspensjon
    select
        vedtak_id,
        sak_id,
        kravhode_id,
        k_sak_t,
        dato_lopende_fom,
        dato_lopende_tom
    from {{ ref('stg_t_vedtak') }}
    -- from pen.t_vedtak
),

ref_kravhode as (
    select
        sak_id,
        kravhode_id,
        k_regelverk_t,
        k_afp_t
    from {{ ref('stg_t_kravhode') }}
    -- from pen.t_kravhode
),

ref_ytelse_komp as (
    -- supertabell for alle ytelsene med beløp
    select
        k_ytelse_komp_t,
        k_minstepen_niva,
        ap_kap19_med_gjr,
        ap_kap19_uten_gjr,
        pen_under_utbet_id,
        bruk,
        netto,
        opphort
    from {{ ref('stg_t_ytelse_komp') }}
    -- from pen.t_ytelse_komp
),

ref_beregning_res as (
    -- kapittel 19
    select
        vedtak_id,
        dato_virk_fom,
        dato_virk_tom,
        beregning_info_id,
        pen_under_utbet_id,
        beregning_info_avdod,
        ber_res_ap_2011_2016_id,
        ber_res_ap_2025_2016_id
    from {{ ref('stg_t_beregning_res') }}
    -- from pen.t_beregning_res
),

ref_uttaksgrad as (
    select
        kravhode_id,
        dato_virk_fom,
        dato_virk_tom,
        uttaksgrad
    from {{ ref('stg_t_uttaksgrad') }}
    -- from pen.t_uttaksgrad
),

ref_pen_under_utbet as (
    select
        pen_under_utbet_id,
        total_belop_brutto,
        total_belop_netto
    from {{ ref('stg_t_pen_under_utbet') }}
    -- from pen.t_pen_under_utbet
),

-- herfra joines inn en og en ref med vedtak

aktive_alder_vedtak as (
    select
        ref_vedtak.sak_id,
        ref_vedtak.vedtak_id,
        ref_vedtak.kravhode_id,
        ref_vedtak.k_sak_t,
        ref_vedtak.dato_lopende_fom,
        ref_vedtak.dato_lopende_tom
    from ref_vedtak
    where
        ref_vedtak.k_sak_t = 'ALDER'
        and ref_vedtak.dato_lopende_fom <= current_date
        and (ref_vedtak.dato_lopende_tom is null or ref_vedtak.dato_lopende_tom >= trunc(current_date))
),

join_kravhode as (
    select
        aktive_alder_vedtak.*,
        ref_kravhode.k_afp_t,
        coalesce(ref_kravhode.k_regelverk_t, 'G_REG') as regelverk
    from aktive_alder_vedtak
    inner join ref_kravhode
        on
            aktive_alder_vedtak.kravhode_id = ref_kravhode.kravhode_id
            and aktive_alder_vedtak.sak_id = ref_kravhode.sak_id
            -- todo: sjekk om dette er nødvendig med begge id-ene
),

join_uttaksgrad as (
    select
        join_kravhode.*,
        ref_uttaksgrad.uttaksgrad
    from join_kravhode
    inner join ref_uttaksgrad
        on
            join_kravhode.kravhode_id = ref_uttaksgrad.kravhode_id
            and ref_uttaksgrad.uttaksgrad != 0
            and ref_uttaksgrad.dato_virk_fom <= current_date
            and (ref_uttaksgrad.dato_virk_tom is null or ref_uttaksgrad.dato_virk_tom >= trunc(current_date))
),

join_beregning_res as (
    select
        join_uttaksgrad.*,
        ref_beregning_res.dato_virk_fom,
        ref_beregning_res.dato_virk_tom,
        ref_beregning_res.beregning_info_id,
        ref_beregning_res.beregning_info_avdod as beregning_info_id_avdod,
        ref_beregning_res.pen_under_utbet_id
    from join_uttaksgrad
    inner join ref_beregning_res
        on
            join_uttaksgrad.vedtak_id = ref_beregning_res.vedtak_id
            and ref_beregning_res.dato_virk_fom <= current_date
            and (ref_beregning_res.dato_virk_tom is null or ref_beregning_res.dato_virk_tom >= trunc(current_date))
),

join_pen_under_utbet as (
    -- todo: sjekke om brutto og netto er samme som i ytelse_komp eller beregning_res
    select
        join_beregning_res.*,
        ref_pen_under_utbet.total_belop_brutto as brutto,
        ref_pen_under_utbet.total_belop_netto as netto
    from join_beregning_res
    inner join ref_pen_under_utbet
        on join_beregning_res.pen_under_utbet_id = ref_pen_under_utbet.pen_under_utbet_id
),

join_beregning_res_2011_og_2025 as (
    select
        join_pen_under_utbet.*,
        br_2011.beregning_info_id as beregning_info_id_2016,
        br_2025.beregning_info_id as beregning_info_id_2025,
        br_2011.beregning_info_avdod as beregning_info_id_avdod_2016
    from join_pen_under_utbet
    left outer join ref_beregning_res br_2011
        on join_pen_under_utbet.beregning_info_id = br_2011.ber_res_ap_2011_2016_id
    left outer join ref_beregning_res br_2025
        on join_pen_under_utbet.beregning_info_id = br_2025.ber_res_ap_2025_2016_id
),


-- prøver å pakke ut ytelse_komp først. Joiner inn på pen_under_utbet_id senere
transponert_ytelse_komp as (
    select
        pen_under_utbet_id,
        sum(case when k_ytelse_komp_t = 'GP' then netto end) as gp_netto,
        sum(case when k_ytelse_komp_t = 'TP' then netto end) as tp_netto,
        sum(case when k_ytelse_komp_t = 'PT' then netto end) as pt_netto,
        sum(case when k_ytelse_komp_t = 'ST' then netto end) as st_netto,
        sum(case when k_ytelse_komp_t = 'IP' then netto end) as ip_netto,
        sum(case when k_ytelse_komp_t = 'ET' then netto end) as et_netto,
        sum(case when k_ytelse_komp_t = 'GAP' then netto end) as gap_netto,
        sum(case when k_ytelse_komp_t = 'AP_GJT' then netto end) as gjt_netto,
        sum(case when k_ytelse_komp_t = 'TSB' then netto end) as saerkull_netto,
        sum(case when k_ytelse_komp_t = 'TFB' then netto end) as barn_felles_netto,
        sum(case when k_ytelse_komp_t = 'SKJERMT' then netto end) as skjermt_netto,
        sum(case when k_ytelse_komp_t = 'AP_GJT_KAP19' then netto end) as gjt_k19_netto,
        sum(case when k_ytelse_komp_t = 'UT_ORDINER' then netto end) as ufor_sum_ut_ord_netto,
        sum(case when k_ytelse_komp_t = 'MIN_NIVA_TILL_INDV' then netto end) as mpn_indiv_netto,
        sum(case when k_ytelse_komp_t = 'MIN_NIVA_TILL_PPAR' then netto end) as mpn_sstot_netto,
        sum(case when k_ytelse_komp_t = 'AP_GJT_KAP19' then ap_kap19_med_gjr end) as ap_kap19_med_gjr_bel,
        sum(case when k_ytelse_komp_t = 'AP_GJT_KAP19' then ap_kap19_uten_gjr end) as ap_kap19_uten_gjr_bel,
        max(k_minstepen_niva) as minste_pen_niva
    from ref_ytelse_komp
    where
        bruk = '1'
        and opphort = '0'
    group by pen_under_utbet_id
),

-- joiner inn ytelse_komp på pen_under_utbet_id
join_ytelse_komp as (
    select
        join_beregning_res_2011_og_2025.*,
        transponert_ytelse_komp.gp_netto,
        transponert_ytelse_komp.tp_netto,
        transponert_ytelse_komp.pt_netto,
        transponert_ytelse_komp.st_netto,
        transponert_ytelse_komp.ip_netto,
        transponert_ytelse_komp.et_netto,
        transponert_ytelse_komp.gap_netto,
        transponert_ytelse_komp.gjt_netto,
        transponert_ytelse_komp.saerkull_netto,
        transponert_ytelse_komp.barn_felles_netto,
        transponert_ytelse_komp.skjermt_netto,
        transponert_ytelse_komp.gjt_k19_netto,
        transponert_ytelse_komp.ufor_sum_ut_ord_netto,
        transponert_ytelse_komp.mpn_indiv_netto,
        transponert_ytelse_komp.mpn_sstot_netto,
        transponert_ytelse_komp.ap_kap19_med_gjr_bel,
        transponert_ytelse_komp.ap_kap19_uten_gjr_bel,
        transponert_ytelse_komp.minste_pen_niva
    from join_beregning_res_2011_og_2025
    left outer join transponert_ytelse_komp
        on join_beregning_res_2011_og_2025.pen_under_utbet_id = transponert_ytelse_komp.pen_under_utbet_id
),

-- legger til kolonner som mangler
ekstra_kolonner as (
    select
        join_ytelse_komp.*,
        'ikke1967' as grein,
        null as beregning_id,
        null as minstepensjonist,
        0 as anvendt_yrkesskade_flagg,
        0 as red_pga_inst_opph_flagg,
        0 as afp_lopph_flagg,
        0 as afp_lopph_ytelse_flagg,
        0 as afp_finans_flagg
    from join_ytelse_komp
),

final as (
    select
        sak_id,
        vedtak_id,
        kravhode_id,
        grein,
        regelverk,
        k_afp_t,
        dato_virk_tom,
        beregning_info_id,
        beregning_info_id_2016,
        beregning_info_id_2025,
        beregning_info_id_avdod,
        beregning_info_id_avdod_2016,
        brutto,
        netto,
        gp_netto,
        tp_netto,
        pt_netto,
        st_netto,
        et_netto,
        saerkull_netto,
        barn_felles_netto,
        mpn_sstot_netto,
        mpn_indiv_netto,
        skjermt_netto,
        ufor_sum_ut_ord_netto,
        gjt_netto,
        gjt_k19_netto,
        ap_kap19_med_gjr_bel,
        ap_kap19_uten_gjr_bel,
        ip_netto,
        gap_netto,
        beregning_id,
        pen_under_utbet_id,
        minste_pen_niva,
        minstepensjonist,
        afp_finans_flagg,
        afp_lopph_flagg,
        afp_lopph_ytelse_flagg,
        anvendt_yrkesskade_flagg,
        red_pga_inst_opph_flagg,
        uttaksgrad
    from ekstra_kolonner
)

select * from final

/*
første versjon fra sql-pilot, hvor alle joins blir gjort i en jafs

yk_bres as (
    -- yk_bres kunne hete ytelseskomponent_kapittel19, er data fra beregning_res
    -- finner aktive utbetalinger fra kaptittel 19
    -- joiner alle ref-ene over
    select
        ref_vedtak.sak_id,
        ref_vedtak.vedtak_id,
        ref_vedtak.kravhode_id,
        ref_vedtak.k_sak_t,
        ref_vedtak.dato_lopende_fom,
        ref_vedtak.dato_lopende_tom,
        yk.pen_under_utbet_id,
        sum(case when yk.k_ytelse_komp_t = 'GP' then yk.netto end) as gp_netto,
        sum(case when yk.k_ytelse_komp_t = 'TP' then yk.netto end) as tp_netto,
        sum(case when yk.k_ytelse_komp_t = 'PT' then yk.netto end) as pt_netto,
        sum(case when yk.k_ytelse_komp_t = 'ST' then yk.netto end) as st_netto,
        sum(case when yk.k_ytelse_komp_t = 'IP' then yk.netto end) as ip_netto,
        sum(case when yk.k_ytelse_komp_t = 'GAP' then yk.netto end) as gap_netto,
        sum(case when yk.k_ytelse_komp_t = 'AP_GJT' then yk.netto end) as gjt_netto,
        sum(case when yk.k_ytelse_komp_t = 'ET' then yk.netto else 0 end) as et_netto,
        sum(case when yk.k_ytelse_komp_t = 'SKJERMT' then yk.netto end) as skjermt_netto,
        sum(case when yk.k_ytelse_komp_t = 'TSB' then yk.netto else 0 end) as saerkull_netto,
        sum(case when yk.k_ytelse_komp_t = 'AP_GJT_KAP19' then yk.netto end) as gjt_k19_netto,
        sum(case when yk.k_ytelse_komp_t = 'TFB' then yk.netto else 0 end) as barn_felles_netto,
        sum(case when yk.k_ytelse_komp_t = 'UT_ORDINER' then yk.netto end) as ufor_sum_ut_ord_netto,
        sum(case when yk.k_ytelse_komp_t = 'MIN_NIVA_TILL_INDV' then yk.netto end) as mpn_indiv_netto,
        sum(case when yk.k_ytelse_komp_t = 'MIN_NIVA_TILL_PPAR' then yk.netto end) as mpn_sstot_netto,
        sum(case when yk.k_ytelse_komp_t = 'AP_GJT_KAP19' then yk.ap_kap19_med_gjr end) as ap_kap19_med_gjr_bel,
        sum(case when yk.k_ytelse_komp_t = 'AP_GJT_KAP19' then yk.ap_kap19_uten_gjr end) as ap_kap19_uten_gjr_bel,
        max(yk.k_minstepen_niva) as minste_pen_niva
    from ref_vedtak  --noqa:ST09
    inner join ref_uttaksgrad ug
    -- velger kun kravhoder som har en aktiv uttaksgrad og ikke er 0
        on
            ug.kravhode_id = ref_vedtak.kravhode_id
            and ug.uttaksgrad != 0
            and ug.dato_virk_fom <= current_date
            and (ug.dato_virk_tom is null or ug.dato_virk_tom >= trunc(current_date))
    inner join ref_beregning_res br
    -- velger velger vedtak_id som har en aktiv beregning_res
        on
            br.vedtak_id = ref_vedtak.vedtak_id
            and br.dato_virk_fom <= current_date
            and (br.dato_virk_tom is null or br.dato_virk_tom >= trunc(current_date))
    inner join ref_pen_under_utbet puu on puu.pen_under_utbet_id = br.pen_under_utbet_id
    -- filtrerer på rader som er i pen_under_utbet, altså er under utbetaling
    inner join ref_ytelse_komp yk
    -- henter beløpene fra ytelse_komp som er i bruk og ikke er opphørt. Bruker også felt herfra
        on
            yk.pen_under_utbet_id = puu.pen_under_utbet_id
            and yk.bruk = '1'
            and yk.opphort = '0'
    where
    -- filtrerer vedtakene på kun alderspensjon og at de er aktive
        ref_vedtak.k_sak_t = 'ALDER'
        and ref_vedtak.dato_lopende_fom <= current_date
        and (ref_vedtak.dato_lopende_tom is null or ref_vedtak.dato_lopende_tom >= trunc(current_date))
    group by
        ref_vedtak.vedtak_id,
        ref_vedtak.sak_id,
        ref_vedtak.kravhode_id,
        ref_vedtak.k_sak_t,
        ref_vedtak.dato_lopende_fom,
        ref_vedtak.dato_lopende_tom,
        ug.uttaksgrad,
        yk.pen_under_utbet_id
),
*/
