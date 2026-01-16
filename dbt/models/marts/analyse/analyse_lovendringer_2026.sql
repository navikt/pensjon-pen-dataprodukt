-- analyse_lopende_ufore

{{
  config(
    materialized = 'table',
    tags = ['analyse']
    )
}}

select
    av.oifu,
    av.oieu,
    av.kompensasjonsgrad,
    av.ugradert_brutto_per_ar,
    ufo.sak_id,
    ufo.vedtak_id,
    ufo.kravhode_id,
    yk.pen_under_utbet_id,
    ufo.dato_opprettet,
    smy.k_minsteytelseniva,
    b2011.egenopptjn_ut_best
from {{ ref('analyse_lopende_ufore') }} ufo
inner join pen.t_ytelse_komp yk
    on
        ufo.pen_under_utbet_id = yk.pen_under_utbet_id
        and yk.k_ytelse_komp_t = 'UT_ORDINER'
        and yk.bruk = '1'
inner join pen.t_avkort_info av
    on
        yk.avkort_info_id = av.avkort_info_id
left join pen.t_minsteytelse my
    on yk.minsteytelse_id = my.minsteytelse_id
left join pen.t_sats_minsteytelse smy
    on my.sats_minsteytelse_id = smy.sats_minsteytelse_id
left join pen.t_beregning_2011 b2011 on ufo.uforetrygd_beregning_id = b2011.beregning_2011_id
