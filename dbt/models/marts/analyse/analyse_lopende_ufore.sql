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
    v.sak_id,
    v.vedtak_id,
    v.dato_opprettet
from pen.t_vedtak v
inner join pen.t_beregning_res br
    on
        v.vedtak_id = br.vedtak_id
        and br.dato_virk_tom is null
inner join pen.t_ytelse_komp yk
    on
        br.pen_under_utbet_id = yk.pen_under_utbet_id
        and yk.k_ytelse_komp_t = 'UT_ORDINER'
        and yk.bruk = '1'
inner join pen.t_avkort_info av
    on
        yk.avkort_info_id = av.avkort_info_id
where
    v.k_sak_t = 'UFOREP'
    and v.dato_lopende_fom is not null
    and v.dato_lopende_tom is null
