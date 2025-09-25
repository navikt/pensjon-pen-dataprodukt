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
),

ref_uttaksgrad as (
    select
        kravhode_id,
        dato_virk_fom,
        dato_virk_tom,
        uttaksgrad
    from {{ ref('stg_t_uttaksgrad') }}
),

ref_pen_under_utbet as (
    select
        pen_under_utbet_id,
        total_belop_brutto,
        total_belop_netto
    from {{ ref('stg_t_pen_under_utbet') }}
),

join_uttaksgrad as (
    select
        ref_int_lopende_vedtak_alder.*,
        ref_uttaksgrad.uttaksgrad
    from ref_int_lopende_vedtak_alder
    inner join ref_uttaksgrad
        on
            ref_int_lopende_vedtak_alder.kravhode_id = ref_uttaksgrad.kravhode_id
            and ref_uttaksgrad.uttaksgrad != 0
            and ref_uttaksgrad.dato_virk_fom <= {{ periode_sluttdato(var("periode")) }}
            and (ref_uttaksgrad.dato_virk_tom is null or ref_uttaksgrad.dato_virk_tom >= trunc({{ periode_sluttdato(var("periode")) }}))
),

join_beregning_res as (
    select
        join_uttaksgrad.*,
        br.pen_under_utbet_id,
        br.beregning_info_id
    from join_uttaksgrad
    inner join ref_beregning_res br
        on
            join_uttaksgrad.vedtak_id = br.vedtak_id
            and br.dato_virk_fom <= {{ periode_sluttdato(var("periode")) }}
            and (br.dato_virk_tom is null or br.dato_virk_tom >= trunc({{ periode_sluttdato(var("periode")) }}))
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
)

select
    vedtak_id,
    sak_id,
    kravhode_id,
    k_sak_t,
    k_regelverk_t,
    dato_lopende_fom,
    dato_lopende_tom,
    uttaksgrad,
    pen_under_utbet_id,
    beregning_info_id,
    brutto,
    netto
from join_pen_under_utbet
