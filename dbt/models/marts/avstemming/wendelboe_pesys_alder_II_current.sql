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
        v.k_regelverk_t,
        {{ dekode('v.k_regelverk_t') }} as regelverk,
        v.k_sak_t,
        {{ dekode('v.k_sak_t') }} as sakstype,
        b.uttaksgrad,
        b.alderspensjon_ytelse_flagg as aldersytelseflagg,
        b.minstepensjon,
        b.brutto,
        b.netto,

        bb.vt_netto,
        bb.pt_netto,
        bb.afp_t_netto,
        bb.afp_livsvarig_netto,
        b.k_bereg_metode_t,
        {{ dekode('b.k_bereg_metode_t') }} as beregning_kode,
        b.k_bor_med_t,
        {{ k_bor_med_t__k_grnl_rolle_t('b.k_bor_med_t') }} as k_grnl_rolle_t,
        bb.minstepen_niva_sats as mpn_arsak_sats,
        b.minstepen_niva_arsak as mpn_arsak_kode,
        -- mpn_aarsak_flagg Vi dropper denne for nå
        case when v.k_regelverk_t = 'G_REG' then 0 else 1 end as nytt_regelverk_flagg,
        case when bb.psats_gp > 0 then 1 else 0 end as gp_avkortet_flagg,
        bb.psats_gp as gp_sats_belop,
        bb.prorata_teller,
        bb.prorata_nevner,
        b.beh_pen_b_totalbelop as beholdning_pensjon_belop,
        b.beh_gar_pen_b_totalbelop as beholdning_garan_pen_belop,
        b.beh_gar_t_b_totalbelop as beholdning_garan_tlg_belop,
        v.inntekt_fpi_belop as inntekt,
        v.inntekt_fpi_belop_eps as inntekt_eps,
        v.eps_aarlig_inntekt,
        bb.sum_fradrag,
        b.gp_restpensjon,
        b.pt_restpensjon,
        b.tp_restpensjon,
        v.kjoretidspunkt

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
