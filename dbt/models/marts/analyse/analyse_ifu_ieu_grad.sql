-- analyse_ifu_ieu_grad

{{
  config(
    materialized = 'table',
    tags = ['analyse']
    )
}}

with

vedtak_med_avkort_info as (
    select * from {{ ref('analyse_lopende_ufore') }}
),

hent_ifu_ieu_grad as (
    select
        bv_ufg.grad,
        bv_ifu.inntekt as ifu, -- opprinnelig ifu, ikke oppjustert
        bv_ieu.inntekt as ieu,
        case when bv_ieu.k_beregning_vilkar_t = 'INNTEKT_ETTER_UFOR' then 'IEU' end as vv_ieu,
        -- bv_ifu.angitt_inntekt,
        bv_ifu.k_minimum_ifu_t, -- MINIMUM_IFU_ENSLIG, MINIMUM_IFU_GIFT, MINIMUM_IFU_UNGUFOR
        bv_ifu.er_minimums_ifu,
        v.*
    from vedtak_med_avkort_info v
    left join pen.t_vilkar_vedtak vv
        on
            v.vedtak_id = vv.vedtak_id
            and vv.dato_virk_tom is null
            and vv.k_kravlinje_t = 'UT'
    inner join pen.t_beregning_vilkar bv_ifu
        on
            vv.vilkar_vedtak_id = bv_ifu.vilkar_vedtak_id
            and bv_ifu.k_beregning_vilkar_t = 'INNTEKT_FOR_UFORHET'
    inner join pen.t_beregning_vilkar bv_ufg
        on
            vv.vilkar_vedtak_id = bv_ufg.vilkar_vedtak_id
            and bv_ufg.k_beregning_vilkar_t = 'UFOREGRAD'
    left join pen.t_beregning_vilkar bv_ieu
        on
            vv.vilkar_vedtak_id = bv_ieu.vilkar_vedtak_id
            and bv_ieu.k_beregning_vilkar_t = 'INNTEKT_ETTER_UFOR'
),

final as (
    select
        grad,
        round((ifu - ieu) / ifu * 100, 4) as beregnet_grad,
        case when oifu > 0 then round((oifu - oieu) / oifu * 100, 4) end as ny_beregnet_grad,
        round(((455560 - oieu) / 455560) * 100 / 5) * 5 as ny_avrundet_grad,
        ifu,
        ieu,
        oifu,
        oieu,
        vv_ieu, -- satt til 'IEU' dersom ieu finnes på vv, ellers null
        k_minimum_ifu_t, -- MINIMUM_IFU_ENSLIG, MINIMUM_IFU_GIFT, MINIMUM_IFU_UNGUFOR
        er_minimums_ifu, -- kombinasjon av denne plutt MINIMUM_IFU_GIFT er målgruppen
        k_minsteytelseniva,
        vedtak_id,
        sak_id,
        dato_opprettet
    from hent_ifu_ieu_grad
)

select * from final
