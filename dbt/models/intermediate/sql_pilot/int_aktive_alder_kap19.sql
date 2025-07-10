-- int_aktive_alder_kap19

-- finner aktive utbetalinger på alder fra kapittel 19
-- bruker da beregning og finner utbetalinger fra ytelse_komp

-- fra SQL-piloten:
-- CTE for å trekke ut de aktive vedtakene, basert på Magne Nordås' SQL-er.
-- Denne kjører løpet via t_beregning, ned mot ytelse_komp.
-- Rader fra ytelse_komp traverseres og transponeres med aggregeringsfunksjoner opp på en rad per vedtak.


with

ref_vedtak as (
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
        beregning_id,
        bruk,
        netto,
        opphort
    from {{ ref('stg_t_ytelse_komp') }}
    -- from pen.t_ytelse_komp
),

ref_beregning as (
    -- kapittel 19
    select
        vedtak_id,
        beregning_id,
        total_vinner,
        dato_virk_fom,
        dato_virk_tom,
        k_minstepensj_t,
        red_pga_inst_opph,
        brutto,
        netto,
        yug,
        tt_anv
    from {{ ref('stg_t_beregning') }}
    -- from pen.t_beregning
),

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
        and ref_vedtak.dato_lopende_fom <= to_date({{ var("periode") }}, 'YYYYMMDD')
        and (ref_vedtak.dato_lopende_tom is null or ref_vedtak.dato_lopende_tom >= to_date({{ var("periode") }}, 'YYYYMMDD'))
),

join_kravhode as (
    select
        aktive_alder_vedtak.*,
        ref_kravhode.k_afp_t,
        ref_kravhode.k_regelverk_t
        -- coalesce(ref_kravhode.k_regelverk_t, 'G_REG') as regelverk -- flyttet ned
    from aktive_alder_vedtak
    inner join ref_kravhode
        on aktive_alder_vedtak.kravhode_id = ref_kravhode.kravhode_id
),

join_beregning as (
    -- har riktig antall rader sammenliknet med yk_ber, sjekket mot Q1
    select
        join_kravhode.*,
        ref_beregning.beregning_id,
        ref_beregning.dato_virk_fom,
        ref_beregning.dato_virk_tom,
        ref_beregning.k_minstepensj_t,
        ref_beregning.red_pga_inst_opph,
        ref_beregning.brutto,
        ref_beregning.netto,
        ref_beregning.yug,
        ref_beregning.tt_anv
    from join_kravhode
    inner join ref_beregning
        on
            join_kravhode.vedtak_id = ref_beregning.vedtak_id
            and ref_beregning.total_vinner = '1'
            and ref_beregning.dato_virk_fom <= to_date({{ var("periode") }}, 'YYYYMMDD')
            and (ref_beregning.dato_virk_tom is null or ref_beregning.dato_virk_tom >= to_date({{ var("periode") }}, 'YYYYMMDD'))
),

transponert_ytelse_komp as (
    select
        beregning_id,
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
        max(k_minstepen_niva) as minste_pen_niva --todo
    from ref_ytelse_komp
    where
        bruk = '1'
        and opphort = '0'
    group by beregning_id -- en rad per type ytelse
),

join_ytelse_komp as (
    select
        join_beregning.*,
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
    from join_beregning
    left outer join transponert_ytelse_komp
        on join_beregning.beregning_id = transponert_ytelse_komp.beregning_id
),

ekstra_kolonner as (
    select
        join_ytelse_komp.*,
        join_ytelse_komp.red_pga_inst_opph as red_pga_inst_opph_flagg,
        cast('1967' as varchar2(8)) as grein,
        100 as uttaksgrad,
        -1 as pen_under_utbet_id, -- todo
        -1 as beregning_info_id, -- todo
        -1 as beregning_info_id_2016, -- todo
        -1 as beregning_info_id_2025, -- todo
        -1 as beregning_info_id_avdod, -- todo
        -1 as beregning_info_id_avdod_2016, -- todo
        case
            when join_ytelse_komp.k_minstepensj_t = 'IKKE_MINST_PEN' then '0'
            when join_ytelse_komp.k_minstepensj_t = 'ER_MINST_PEN' then '1'
        end as minstepensjonist,
        case
            when coalesce(join_ytelse_komp.yug, 0) > 0 then 1
            else 0
        end as anvendt_yrkesskade_flagg,
        case
            when join_ytelse_komp.k_sak_t = 'AFP' then 1
            else 0
        end as afp_lopph_flagg,
        case
            when
                join_ytelse_komp.k_sak_t = 'AFP'
                and coalesce(join_ytelse_komp.netto, 0) > 0 then 1
            else 0
        end as afp_lopph_ytelse_flagg,
        case
            when
                join_ytelse_komp.k_sak_t = 'AFP'
                and coalesce(join_ytelse_komp.netto, 0) > 0
                and join_ytelse_komp.k_afp_t = 'FINANS' then 1
            else 0
        end as afp_finans_flagg,
        coalesce(join_ytelse_komp.k_regelverk_t, 'G_REG') as regelverk
    from join_ytelse_komp
),

final as (
    select
        sak_id,
        vedtak_id,
        kravhode_id,
        grein,
        k_afp_t,
        k_sak_t,
        regelverk,
        dato_virk_tom,
        dato_virk_fom,
        beregning_id,
        beregning_info_id,
        beregning_info_id_2016,
        beregning_info_id_2025,
        beregning_info_id_avdod,
        beregning_info_id_avdod_2016,
        pen_under_utbet_id,
        uttaksgrad,
        brutto,
        netto,
        gp_netto,
        tp_netto,
        pt_netto,
        st_netto,
        et_netto,
        ip_netto,
        gap_netto,
        gjt_netto,
        gjt_k19_netto,
        skjermt_netto,
        saerkull_netto,
        mpn_sstot_netto,
        mpn_indiv_netto,
        barn_felles_netto,
        ufor_sum_ut_ord_netto,
        ap_kap19_uten_gjr_bel,
        ap_kap19_med_gjr_bel,
        minstepensjonist,
        minste_pen_niva,
        afp_lopph_flagg,
        afp_finans_flagg,
        afp_lopph_ytelse_flagg,
        red_pga_inst_opph_flagg,
        anvendt_yrkesskade_flagg
    from ekstra_kolonner
)

select * from final
