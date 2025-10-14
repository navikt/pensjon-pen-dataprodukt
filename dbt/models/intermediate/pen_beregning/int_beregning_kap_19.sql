with

ref_int_lopende_vedtak_alder as (
    select
        vedtak_id,
        sak_id,
        kravhode_id,
        k_sak_t,
        dato_lopende_fom,
        dato_lopende_tom,
        k_regelverk_t
    from {{ ref('int_lopende_vedtak_alder') }}
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
        k_minstepensj_arsak,
        red_pga_inst_opph,
        brutto,
        netto,
        yug,
        tt_anv,
        k_bereg_metode_t
    from {{ ref('stg_t_beregning') }}
    -- from pen.t_beregning
),

join_beregning as (
    select
        ref_int_lopende_vedtak_alder.*,
        ref_beregning.beregning_id,
        ref_beregning.k_minstepensj_t,
        ref_beregning.k_minstepensj_arsak,
        ref_beregning.red_pga_inst_opph,
        ref_beregning.brutto,
        ref_beregning.netto,
        ref_beregning.yug,
        ref_beregning.tt_anv,
        ref_beregning.k_bereg_metode_t
    from ref_int_lopende_vedtak_alder
    inner join ref_beregning
        on
            ref_int_lopende_vedtak_alder.vedtak_id = ref_beregning.vedtak_id
            and ref_beregning.total_vinner = '1'
            and ref_beregning.dato_virk_fom <= {{ periode_sluttdato(var("periode")) }}
            and (ref_beregning.dato_virk_tom is null or ref_beregning.dato_virk_tom >= trunc({{ periode_sluttdato(var("periode")) }}))
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

    beregning_id,

    k_bereg_metode_t,
    k_minstepensj_t,
    k_minstepensj_arsak,
    red_pga_inst_opph,
    yug,
    tt_anv
from join_beregning
