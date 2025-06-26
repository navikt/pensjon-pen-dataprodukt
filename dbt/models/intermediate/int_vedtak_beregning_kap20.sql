with

ref_int_vedtak_alder as (
    select
        vedtak_id,
        kravhode_id,
        dato_virk_fom,
        dato_virk_tom,
        dato_lopende_fom,
        dato_lopende_tom
    from {{ ref('int_vedtak_alder') }}
),

ref_stg_uttaksgrad as (
    select
        kravhode_id,
        sak_id,
        dato_virk_fom,
        dato_virk_tom,
        uttaksgrad
    from {{ ref('stg_t_uttaksgrad') }}
),

ref_stg_t_beregning_res as (
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

ref_stg_t_pen_under_utbet as (
    select
        pen_under_utbet_id,
        total_belop_brutto,
        total_belop_netto
    from {{ ref('stg_t_pen_under_utbet') }}
),


ref_stg_t_ytelse_komp as (
    select
        ytelse_komp_id,
        k_ytelse_komp_t,
        k_minstepen_niva,
        ap_kap19_med_gjr,
        ap_kap19_uten_gjr,
        pen_under_utbet_id,
        beregning_id,
        bruk,
        netto,
        opphort
    from {{ ref('stg_t_ytelse_komp') }}
),


yk_bres as (
    select
        v.*
    from ref_int_vedtak_alder v
    inner join ref_stg_uttaksgrad ug on ug.kravhode_id = v.kravhode_id
        and ug.dato_virk_fom <= current_date
        and (ug.dato_virk_tom is null or ug.dato_virk_tom >= trunc(current_date))
        and ug.uttaksgrad <> 0
    inner join ref_stg_t_beregning_res br on br.vedtak_id = v.vedtak_id
        and br.dato_virk_fom <= current_date
        and (br.dato_virk_tom is null or br.dato_virk_tom >= trunc(current_date))
    inner join ref_stg_t_pen_under_utbet puu on puu.pen_under_utbet_id = br.pen_under_utbet_id
    inner join ref_stg_t_ytelse_komp yk on yk.pen_under_utbet_id = puu.pen_under_utbet_id
        and yk.bruk = '1'
        and yk.opphort = '0'
)

select * from yk_bres