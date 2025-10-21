with

ref_int_beregning as (
    select * from {{ ref('dataprodukt_alderspensjon_beregning') }}
),

ref_int_beregning_belop as (
    select * from {{ ref('dataprodukt_alderspensjon_belop') }}
),

ref_int_vedtaksinfo as (
    select * from {{ ref('dataprodukt_alderspensjon_vedtak') }}
),

dataprodukt_2 as (
    select
        v.periode,
        v.vedtak_id,
        v.sak_id,
        v.kravhode_id,
        null as persnr,
        v.person_id,
        v.dato_lopende_fom,
        v.dato_lopende_tom,
        v.k_regelverk_t as regelverk,
        v.k_sak_t as sakstype,
        b.uttaksgrad,
        b.alderspensjon_ytelse_flagg as aldersytelseflagg,
        b.minstepensjon,
        b.brutto,
        b.netto,

        bb.vt_netto,
        bb.pt_netto,
        bb.afp_t_netto,
        bb.afp_livsvarig_netto,
        b.k_bereg_metode_t as beregning_kode,
        b.k_bor_med_t,
        --k_grnl_rolle_t,
        bb.k_minstepen_niva, -- mpn_arsak_sats, mpn_aarsak_kode, mpn_aarsak_flagg,
        bb.minstepen_niva_sats, -- mpn_arsak_sats, mpn_aarsak_kode, mpn_aarsak_flagg,
        b.minstepen_niva_arsak, -- mpn_arsak_sats, mpn_aarsak_kode, mpn_aarsak_flagg,

        case when v.k_regelverk_t != 'G_REG' then 0 else 1 end as nytt_regelverk_flagg,
        bb.gp_avkorting_flagg as gp_avkortet_flagg,
        -- gp_sats_belop, Legges til fra YK
        -- prorata_teller,
        -- prorata_nevner,
        b.beh_pen_b_totalbelop as beholdning_pensjon_belop,
        b.beh_gar_pen_b_totalbelop as beholdning_garan_pen_belop,
        b.beh_gar_t_b_totalbelop as beholdning_garan_tlg_belop,
        v.inntekt,
        v.inntekt_eps,
        v.eps_aarlig_inntekt,
        bb.sum_fradrag,
        b.gp_restpensjon,
        b.pt_restpensjon,
        b.tp_restpensjon
        --kjoretidspunkt

    from ref_int_vedtaksinfo v
    inner join ref_int_beregning b
        on
            v.vedtak_id = b.vedtak_id
            and v.periode = b.periode
    inner join ref_int_beregning_belop bb
        on
            v.vedtak_id = bb.vedtak_id
            and v.periode = bb.periode
)

select * from dataprodukt_2
