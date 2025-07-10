-- int_overgangsstonad
-- todo partition by ref_int_aktive_alder.vedtak_id order by ref_int_aktive_alder.vedtak_id gir ikke mening
-- setter flagget overgangsstonad, basert på seed_overgangsstonad og vilkår vedtak
-- sql-pilot linje 381-393


with

ref_int_aktive_alder as (
    -- skal legge til kolonnen overgangsstonad til resten
    -- joiner på vedtak_id
    select *
    from {{ ref('int_aktive_alder') }}
    -- from int_aktive_alder_kap19
),

ref_vilkar_vedtak as (
    select
        vedtak_id,
        k_vilk_vurd_t,
        dato_virk_fom,
        dato_virk_tom
    from {{ ref('stg_t_vilkar_vedtak') }}
    -- from pen.t_vilkar_vedtak
),

ref_seed_overgangsstonad as (
    select
        kode,
        flagg
    from {{ ref('seed_overgangsstonad') }}
    -- from seed_overgangsstonad
),


-- er sånn den er i sql-pilot
-- jeg skjønner ikke helt order by her. kopiert fra sql-pilot
tvvx as (
    select
        vedtak_id,
        k_vilk_vurd_t
    from (
        select
            ref_int_aktive_alder.vedtak_id,
            ref_vilkar_vedtak.k_vilk_vurd_t,
            row_number()
                over (
                    partition by ref_int_aktive_alder.vedtak_id
                    order by ref_int_aktive_alder.vedtak_id
                ) as rn
        from ref_int_aktive_alder
        left outer join ref_vilkar_vedtak
            on ref_int_aktive_alder.vedtak_id = ref_vilkar_vedtak.vedtak_id
        where
            ref_vilkar_vedtak.dato_virk_fom <= to_date({{ var("periode") }}, 'YYYYMMDD')
            and (ref_vilkar_vedtak.dato_virk_tom >= to_date({{ var("periode") }}, 'YYYYMMDD') or ref_vilkar_vedtak.dato_virk_tom is null)
    )
    where rn = 1
),

sette_flagg_overgangsstonad as (
    select
        ref_int_aktive_alder.*,
        case
            when
                ref_seed_overgangsstonad.flagg = 1
                and ref_int_aktive_alder.netto > 0
                then 1
            else 0
        end as overgangsstonad -- burde vært overgangsstonad_flagg
    from ref_int_aktive_alder -- noqa: ST09
    left outer join tvvx
        on ref_int_aktive_alder.vedtak_id = tvvx.vedtak_id
    left outer join ref_seed_overgangsstonad
        on ref_seed_overgangsstonad.kode = tvvx.k_vilk_vurd_t
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
        anvendt_yrkesskade_flagg,
        overgangsstonad
    from sette_flagg_overgangsstonad
)

select * from final

/*
distinct k_vilk_vurd_t i tvvx i Q1 er:
    INNV2_GJR
    INNV3_GJR
    HALV_MINPEN_GJR
    (null)
    INNV1_GJR
    OMSTILL_GJR
    INNV5_GJR
    EGNE_BARN_KUN_GJR
    INNV4_GJR
    EGNE_BARN_GJR
*/
