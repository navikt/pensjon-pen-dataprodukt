-- union av kap20 og kap 19
-- har midlertidige placeholder-verdier for noen felter

with

kap19 as (
    select
        vedtak_id,
        sak_id,
        kravhode_id,
        k_sak_t,
        k_regelverk_t,
        dato_lopende_fom,
        dato_lopende_tom,

        brutto,
        netto,

        beregning_id,

        k_minstepensj_t,
        red_pga_inst_opph,
        yug,
        tt_anv
    from {{ ref('int_beregning_kap_19') }}
),

kap20 as (
    select
        vedtak_id,
        sak_id,
        kravhode_id,
        k_sak_t,
        dato_lopende_fom,
        dato_lopende_tom,
        k_regelverk_t,

        uttaksgrad,
        --beregning_res_id,
        pen_under_utbet_id,
        --beregning_info_id,
        mottar_min_pensjonsniva,
        inst_opph_anv,
        gjenlevrett_anv,
        yrksk_anv,
        yrksk_grad,
        tt_anv_g_opptj,
        tt_anv_n_opptj,
        brutto,
        netto
    from {{ ref('int_beregning_kap_20') }}
),

kap19_nye_felter as (
    select
        vedtak_id,
        sak_id,
        kravhode_id,
        k_sak_t,
        k_regelverk_t,
        dato_lopende_fom,
        dato_lopende_tom,
        brutto,
        netto,

        beregning_id,
        null as pen_under_utbet_id,

        100 as uttaksgrad,
        red_pga_inst_opph as institusjon_opphold,
        case when yug > 0 then '1' else '0' end as anvendt_yrkesskade_flagg,
        tt_anv as tt_anv_g_opptj,
        null as tt_anv_n_opptj,
        null as gjenlevrett_anv, -- todo: burde ikke denne vært satt for kap 19, og ikke bare for kap20?
        case when k_minstepensj_t = 'ER_MINST_PEN' then '1' else '0' end as minstepensjon
        -- ...
        -- her kommer flere felter fra t_beregning og placeholdere fra t_beregning_info
        -- ...
    from kap19
),

kap20_nye_felter as (
    select
        vedtak_id,
        sak_id,
        kravhode_id,
        k_sak_t,
        k_regelverk_t,
        dato_lopende_fom,
        dato_lopende_tom,
        brutto,
        netto,

        pen_under_utbet_id,
        null as beregning_id,

        uttaksgrad,
        inst_opph_anv as institusjon_opphold,
        case
            when yrksk_anv = '1' and yrksk_grad > 0 then '1'
            else '0'
        end as anvendt_yrkesskade_flagg,
        tt_anv_g_opptj,
        tt_anv_n_opptj,
        gjenlevrett_anv,
        case when mottar_min_pensjonsniva = '1' then '1' else '0' end as minstepensjon -- heller coalesce()
        -- ...
        -- her kommer flere felter fra t_beregning_info og placeholdere fra t_beregning
        -- ...
    from kap20
),

union_beregning as (
    select
        vedtak_id,
        sak_id,
        kravhode_id,
        k_sak_t,
        k_regelverk_t,
        dato_lopende_fom,
        dato_lopende_tom,
        brutto,
        netto,

        pen_under_utbet_id,
        beregning_id,

        uttaksgrad,
        institusjon_opphold,
        anvendt_yrkesskade_flagg,
        tt_anv_g_opptj,
        tt_anv_n_opptj,
        gjenlevrett_anv,
        minstepensjon
    from kap19_nye_felter
    union all
    select
        vedtak_id,
        sak_id,
        kravhode_id,
        k_sak_t,
        k_regelverk_t,
        dato_lopende_fom,
        dato_lopende_tom,
        brutto,
        netto,

        pen_under_utbet_id,
        beregning_id,

        uttaksgrad,
        institusjon_opphold,
        anvendt_yrkesskade_flagg,
        tt_anv_g_opptj,
        tt_anv_n_opptj,
        gjenlevrett_anv,
        minstepensjon
    from kap20_nye_felter
)

select
    vedtak_id,
    sak_id,
    kravhode_id,
    k_sak_t,
    k_regelverk_t,
    dato_lopende_fom,
    dato_lopende_tom,
    brutto,
    netto,

    pen_under_utbet_id,
    beregning_id,

    uttaksgrad,
    tt_anv_g_opptj,
    tt_anv_n_opptj,

    -- flagg
    cast(institusjon_opphold as varchar2(1)) as institusjon_opphold,
    cast(anvendt_yrkesskade_flagg as varchar2(1)) as anvendt_yrkesskade_flagg,
    cast(gjenlevrett_anv as varchar2(1)) as gjenlevrett_anv,
    cast(minstepensjon as varchar2(1)) as minstepensjon
from union_beregning
