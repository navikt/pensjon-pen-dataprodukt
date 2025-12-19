-- int_beregning
-- union av kap20 og kap 19
-- har midlertidige placeholder-verdier for noen felter

{{
  config(
    materialized = 'table',
    )
}}

with

ref_stg_t_vilkar_vedtak as (
    select
        vedtak_id,
        k_kravlinje_t,
        k_vilkar_resul_t,
        dato_virk_fom,
        dato_virk_tom
    from pen.t_vilkar_vedtak
),

kap19 as (
    select
        sak_id,
        vedtak_id,
        k_regelverk_t,
        brutto,
        netto,

        beregning_id,

        -- felles
        yug,
        tt_anv,
        red_pga_inst_opph,
        k_minstepensj_t,
        k_minstepensj_arsak,
        k_bereg_metode_t,
        k_bor_med_t,
        k_resultat_t
    from {{ ref('int_beregning_kap_19') }}
),

kap20 as (
    select
        sak_id,
        vedtak_id,
        k_regelverk_t,
        brutto,
        netto,

        pen_under_utbet_id,
        --beregning_res_id,
        --beregning_info_id,

        -- kun kap20
        uttaksgrad,
        gjenlevrett_anv,
        rett_pa_gjlevenderett,
        tp_restpensjon,
        pt_restpensjon,
        gp_restpensjon,

        -- felles
        yrksk_anv,
        yrksk_grad,
        yrksk_reg,
        yrksk_reg_avdod,
        yrksk_anv_avdod,
        tt_anv_g_opptj,
        tt_anv_n_opptj,
        inst_opph_anv,
        mottar_min_pensjonsniva,
        mottar_min_pensjniva_arsak,
        k_bereg_metode_t,
        k_bor_med_t,
        beh_pen_b_totalbelop,
        beh_gar_pen_b_totalbelop,
        beh_gar_t_b_totalbelop
    from {{ ref('int_beregning_kap_20') }}
),

union_beregning as (
    select
        vedtak_id,
        sak_id,
        k_regelverk_t,
        brutto,
        netto,

        null as pen_under_utbet_id,
        beregning_id,

        case
            when netto <= 0 or brutto <= 0 or netto > brutto then 0
            else 100
        end as uttaksgrad, -- i kap 19 eksisterer ikke konseptet uttaksgrad
        red_pga_inst_opph as institusjon_opphold,

        case
            when
                yug > 0
                or k_resultat_t in ('UP_GJP_UP_YP', 'GJP_UP_YP', 'AP_GJP_UP_YP', 'UP_YP', 'AP2011_GJP_UP_YP')
                then '1'
            else '0'
        end as yrkesskade_rett_flagg,
        case when yug > 0 then '1' else '0' end as yrkesskade_anv_flagg,

        tt_anv as tt_anv_g_opptj,
        null as tt_anv_n_opptj,
        null as gjenlevrett_anv, -- todo: burde ikke denne vært satt for kap 19, og ikke bare for kap20?
        null as rett_pa_gjlevenderett,
        case when k_minstepensj_t = 'ER_MINST_PEN' then '1' else '0' end as minstepensjon,
        k_minstepensj_arsak as minstepen_niva_arsak,
        k_bereg_metode_t,
        k_bor_med_t,
        null as tp_restpensjon,
        null as pt_restpensjon,
        null as gp_restpensjon,
        null as beh_pen_b_totalbelop,
        null as beh_gar_pen_b_totalbelop,
        null as beh_gar_t_b_totalbelop
        -- ...
        -- her kommer flere felter fra t_beregning og placeholdere fra t_beregning_info
        -- ...
    from kap19
    union all
    select
        vedtak_id,
        sak_id,
        k_regelverk_t,
        brutto,
        netto,

        pen_under_utbet_id,
        null as beregning_id,

        uttaksgrad,
        inst_opph_anv as institusjon_opphold,
        case
            when yrksk_reg = '1' or (rett_pa_gjlevenderett = '1' and yrksk_reg_avdod = '1') then '1'
            else '0'
        end as yrkesskade_rett_flagg,

        case
            when yrksk_anv = '1' or (rett_pa_gjlevenderett = '1' and yrksk_anv_avdod = '1') then '1'
            else '0'
        end as yrkesskade_anv_flagg,

        tt_anv_g_opptj,
        tt_anv_n_opptj,
        gjenlevrett_anv,
        rett_pa_gjlevenderett,
        case when mottar_min_pensjonsniva = '1' then '1' else '0' end as minstepensjon, -- heller coalesce()
        mottar_min_pensjniva_arsak as minstepen_niva_arsak,
        k_bereg_metode_t,
        k_bor_med_t,
        tp_restpensjon,
        pt_restpensjon,
        gp_restpensjon,
        beh_pen_b_totalbelop,
        beh_gar_pen_b_totalbelop,
        beh_gar_t_b_totalbelop
        -- ...
        -- her kommer flere felter fra t_beregning_info og placeholdere fra t_beregning
        -- ...
    from kap20
),

sett_inv_gj_rett as (
    select
        ber.*,
        case when (vv.vedtak_id is not null) then 1 else 0 end as innv_gj_rett
    from union_beregning ber
    left join
        ref_stg_t_vilkar_vedtak
            vv on ber.vedtak_id = vv.vedtak_id
    and vv.k_kravlinje_t = 'GJR'
    and vv.k_vilkar_resul_t = 'INNV'
    and vv.dato_virk_fom < current_date
    and (
        vv.dato_virk_tom >= trunc(current_date)
        or vv.dato_virk_tom is null
    )

)

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
    cast(institusjon_opphold as varchar2(1)) as institusjon_opphold,
    cast(yrkesskade_anv_flagg as varchar2(1)) as yrkesskade_anv_flagg,
    cast(yrkesskade_rett_flagg as varchar2(1)) as yrkesskade_rett_flagg,
    cast(gjenlevrett_anv as varchar2(1)) as gjenlevrett_anv,
    cast(innv_gj_rett as varchar2(1)) as innv_gj_rett,

    cast(minstepensjon as varchar2(1)) as minstepensjon,
    cast(case when netto > 0 then 1 else 0 end as varchar2(1)) as alderspensjon_ytelse_flagg,

    -- id-er som ikke nødvendigvis skal til sluttproduktet
    pen_under_utbet_id,
    beregning_id
from sett_inv_gj_rett
