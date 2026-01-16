-- analyse_lopende_ufore

{{
  config(
    materialized = 'table',
    tags = ['analyse']
    )
}}

select
    v.sak_id,
    v.vedtak_id,
    v.kravhode_id,
    v.dato_opprettet,
    br.beregning_res_id,
    br.pen_under_utbet_id,
    br.uforetrygd_beregning_id
from pen.t_vedtak v
inner join pen.t_beregning_res br
    on
        v.vedtak_id = br.vedtak_id
        and br.dato_virk_tom is null
where
    v.k_sak_t = 'UFOREP'
    and v.dato_lopende_fom is not null
    and v.dato_lopende_tom is null
