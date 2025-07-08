-- sql_pilot_dbt.sql
-- leser fra intermediate til tabell i marts

-- kobler ikke på personnummer her, det kan være en egen mappe i marts
-- det er uansett ikke relevant enda

{{ config(
    materialized='table',
) }}

with

ref_int_flere_joins_pilot as (
    select *
    from {{ ref('int_flere_joins_pilot') }}
),

final as (
    select
        sak_id,
        vedtak_id,
        kravhode_id,
        cast(null as varchar2(50)) as person_id, --todo
        grein,
        sakstype,
        uttaksgrad,
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
        minste_pen_niva,
        afp_lopph_flagg,
        afp_finans_flagg,
        afp_lopph_ytelse_flagg,
        cast(null as varchar2(50)) as overgangsstonad, -- todo
        afp_privat_flagg,
        gjenlevrett_anv,
        red_pga_inst_opph_flagg,
        tt_anvendt_kap19_antall, -- må skjønne hva disse skal være
        tt_anvendt_kap20_antall, -- må skjønne hva disse skal være
        minstepensjon,
        anvendt_yrkesskade_flagg,
        aldersytelseflagg,
        afp_ordning,
        regelverk,
        kjoretidspunkt,
        periode
    from ref_int_flere_joins_pilot

)

select * from final
