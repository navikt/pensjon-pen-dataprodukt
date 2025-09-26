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
    select
        beregning_res_id,
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

ref_beregning_info as (
    select
        beregning_info_id,
        mottar_min_pensjonsniva,
        tt_anv,
        yrksk_anv,
        gjenlevrett_anv,
        inst_opph_anv
    from {{ ref('stg_t_beregning_info') }}
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
        br.beregning_res_id,
        br.ber_res_ap_2011_2016_id,
        br.ber_res_ap_2025_2016_id,
        br.pen_under_utbet_id,
        br.beregning_info_id
    from join_uttaksgrad
    inner join ref_beregning_res br
        on
            join_uttaksgrad.vedtak_id = br.vedtak_id
            and br.dato_virk_fom <= {{ periode_sluttdato(var("periode")) }}
            and (br.dato_virk_tom is null or br.dato_virk_tom >= trunc({{ periode_sluttdato(var("periode")) }}))
),

join_beregning_info_direkte as (
    select
        join_beregning_res.*,
        ref_beregning_info.tt_anv
        -- , ref_beregning_info.inst_opph ...
    from join_beregning_res
    inner join ref_beregning_info
        on join_beregning_res.beregning_info_id = ref_beregning_info.beregning_info_id
-- where k_regelverk_t inb ('N_REG_G_OPPTJ', 'N_REG_N_OPPTJ') -- 2011 og 2025
),

join_beregning_info_overgang_2016 as (
    select
        join_beregning_res.*,
        -- de har både kap19 og kap 20 beregning_info, men ingen beregning_info direkte på beregning_res
        br_2011.beregning_info_id as beregning_info_id_2011,
        bi_2011.tt_anv as tt_anv_2011,
        bi_2011.mottar_min_pensjonsniva as mottar_min_pensjonsniva_2011,
        bi_2011.inst_opph_anv as inst_opph_anv_2011,
        bi_2011.gjenlevrett_anv as gjenlevrett_anv_2011,
        bi_2011.yrksk_anv as yrksk_anv_2011,

        br_2025.beregning_info_id as beregning_info_id_2025,
        bi_2025.tt_anv as tt_anv_2025,
        bi_2025.mottar_min_pensjonsniva as mottar_min_pensjonsniva_2025,
        bi_2025.inst_opph_anv as inst_opph_anv_2025,
        bi_2025.gjenlevrett_anv as gjenlevrett_anv_2025,
        bi_2025.yrksk_anv as yrksk_anv_2025

    from
        join_beregning_res
        {# join på ber_res_ap_2011_2016_id og ber_res_ap_2025_2016_id #}
        {# deretter join på to beregning_id, en for 2011 og en for 2025 #}
    left join ref_beregning_res br_2011 on join_beregning_res.beregning_res_id = br_2011.ber_res_ap_2011_2016_id
    left join ref_beregning_info bi_2011 on br_2011.beregning_info_id = bi_2011.beregning_info_id

    left join ref_beregning_res br_2025 on join_beregning_res.beregning_res_id = br_2025.ber_res_ap_2025_2016_id
    left join ref_beregning_info bi_2025 on br_2025.beregning_info_id = bi_2025.beregning_info_id
    where
        join_beregning_res.k_regelverk_t = 'N_REG_G_N_OPPTJ'
),

union_beregning_info as (
    select * from join_beregning_info_direkte
    union all
    select * from join_beregning_info_overgang_2016
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

    brutto,
    netto,

    pen_under_utbet_id,
    beregning_info_id,

    uttaksgrad
from join_pen_under_utbet
