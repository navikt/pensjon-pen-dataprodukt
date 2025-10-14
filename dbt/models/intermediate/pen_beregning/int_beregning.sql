-- int_beregning
-- union av kap20 og kap 19
-- har midlertidige placeholder-verdier for noen felter

with

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
        k_bereg_metode_t
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
        tp_restpensjon,
        pt_restpensjon,
        gp_restpensjon,

        -- felles
        yrksk_anv,
        yrksk_grad,
        tt_anv_g_opptj,
        tt_anv_n_opptj,
        inst_opph_anv,
        mottar_min_pensjonsniva,
        k_bereg_metode_t,
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

        100 as uttaksgrad,
        red_pga_inst_opph as institusjon_opphold,
        case when yug > 0 then '1' else '0' end as anvendt_yrkesskade_flagg,
        tt_anv as tt_anv_g_opptj,
        null as tt_anv_n_opptj,
        null as gjenlevrett_anv, -- todo: burde ikke denne vært satt for kap 19, og ikke bare for kap20?
        case when k_minstepensj_t = 'ER_MINST_PEN' then '1' else '0' end as minstepensjon,
        k_bereg_metode_t,
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
            when yrksk_anv = '1' and yrksk_grad > 0 then '1'
            else '0'
        end as anvendt_yrkesskade_flagg,
        tt_anv_g_opptj,
        tt_anv_n_opptj,
        gjenlevrett_anv,
        case when mottar_min_pensjonsniva = '1' then '1' else '0' end as minstepensjon, -- heller coalesce()
        k_bereg_metode_t,
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
    tp_restpensjon,
    pt_restpensjon,
    gp_restpensjon,
    beh_pen_b_totalbelop,
    beh_gar_pen_b_totalbelop,
    beh_gar_t_b_totalbelop,

    -- flagg
    cast(institusjon_opphold as varchar2(1)) as institusjon_opphold,
    cast(anvendt_yrkesskade_flagg as varchar2(1)) as anvendt_yrkesskade_flagg,
    cast(gjenlevrett_anv as varchar2(1)) as gjenlevrett_anv,
    cast(minstepensjon as varchar2(1)) as minstepensjon,
    case
        when netto > 0 then 1
        else 0
    end as alderspensjon_ytelse_flagg,

    -- id-er som ikke nødvendigvis skal til sluttproduktet
    pen_under_utbet_id,
    beregning_id
from union_beregning
