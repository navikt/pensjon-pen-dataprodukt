{{
  config(
    materialized = 'table',
    )
}}

with

vedtak as (
    select * from {{ ref('stg_t_vedtak') }}
),

beregning_res as (
    select * from {{ ref('stg_t_beregning_res') }}
),

ytelse_komp as (
    select * from {{ ref('stg_t_ytelse_komp') }}
)

select
    v.sak_id,
    v.vedtak_id,
    v.kravhode_id,
    v.ufore_historik_id,
    br.pen_under_utbet_id,
    br.beregning_res_id,
    br.uforetrygd_beregning_id,
    yk.ytelse_komp_id,
    yk.avkort_info_id

from vedtak v
inner join beregning_res br
    on
        v.vedtak_id = br.vedtak_id
        and br.dato_virk_tom is null
inner join ytelse_komp yk
    on
        br.pen_under_utbet_id = yk.pen_under_utbet_id
        and yk.bruk = '1'
        and yk.k_ytelse_komp_t = 'UT_ORDINER'
where
    v.k_sak_t = 'UFOREP'
    and v.dato_lopende_fom is not null
    and v.dato_lopende_tom is null
