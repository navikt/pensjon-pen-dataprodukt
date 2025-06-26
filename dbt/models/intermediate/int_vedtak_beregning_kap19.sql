with

ref_int_vedtak_alder as (
    select
        vedtak_id,
        dato_virk_fom,
        dato_virk_tom,
        dato_lopende_fom,
        dato_lopende_tom
    from {{ ref('int_vedtak_alder') }}
),

ref_stg_t_beregning as (
    select
        vedtak_id,
        beregning_id,
        total_vinner,
        dato_virk_fom,
        dato_virk_tom,
        k_minstepensj_t,
        red_pga_inst_opph,
        brutto,
        netto,
        yug,
        tt_anv
    from {{ ref('stg_t_beregning') }}
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


yk_ber as (
    select
        v.*
    from ref_int_vedtak_alder v
    inner join ref_stg_t_beregning b on b.vedtak_id = v.vedtak_id
        and b.total_vinner = '1'
        and b.dato_virk_fom <= current_date
        and (b.dato_virk_tom is null or b.dato_virk_tom >= trunc(current_date))
    inner join ref_stg_t_ytelse_komp yk on yk.beregning_id = b.beregning_id
        and yk.bruk = '1'
        and yk.opphort = '0'
    --group by v.vedtak_id, v.sak_id, v.kravhode_id, yk.beregning_id
)

select * from yk_ber