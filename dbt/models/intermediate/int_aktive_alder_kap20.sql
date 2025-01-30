-- int_aktive_alder_kap20

-- finner aktive utbetalinger på alder fra kapittel 20
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
),

ref_ytelse_komp as (
    -- supertabell for alle ytelsene med beløp
    select
        k_ytelse_komp_t,
        pen_under_utbet_id,
        ap_kap19_med_gjr,
        ap_kap19_uten_gjr,
        bruk,
        k_minstepen_niva,
        netto,
        opphort
    from {{ ref('stg_t_ytelse_komp') }}
),

ref_beregning as (
    -- kapittel 20
    select
        vedtak_id,
        beregning_id,
        total_vinner,
        dato_virk_fom,
        dato_virk_tom
    from {{ ref('stg_t_beregning') }}
),

yk_ber as (
    select
        ref_vedtak.vedtak_id,
        ref_vedtak.sak_id,
        ref_vedtak.kravhode_id,
        yk.beregning_id,
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
    inner join ref_beregning
    -- todo: skrive noe smart om denne joinen
        on
            ref_beregning.vedtak_id = ref_vedtak.vedtak_id
            and ref_beregning.total_vinner = '1'
            and ref_beregning.dato_virk_fom <= current_date
            and (ref_beregning.dato_virk_tom is null or ref_beregning.dato_virk_tom >= trunc(current_date))
    inner join ref_ytelse_komp yk
    -- henter beløpene fra ytelse_komp som er i bruk og ikke er opphørt. Bruker også felt herfra
        on
            yk.beregning_id = ref_beregning.beregning_id
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
        yk.beregning_id
),

final as (
    select
        sak_id,
        vedtak_id,
        kravhode_id,
        gp_netto,
        tp_netto,
        pt_netto,
        st_netto,
        ip_netto,
        gap_netto,
        gjt_netto,
        et_netto,
        skjermt_netto,
        saerkull_netto,
        gjt_k19_netto,
        barn_felles_netto,
        ufor_sum_ut_ord_netto,
        mpn_indiv_netto,
        mpn_sstot_netto,
        ap_kap19_med_gjr_bel,
        ap_kap19_uten_gjr_bel,
        minste_pen_niva
    from yk_ber
)

select * from final
