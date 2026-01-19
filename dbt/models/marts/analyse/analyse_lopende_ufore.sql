-- analyse_lopende_ufore

{{
  config(
    materialized = 'table',
    tags = ['analyse']
    )
}}

with

ufore_vedtak as (
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
),

forste_virk as (
    select
        ufore_vedtak.sak_id,
        min(v.dato_virk_fom) as forste_virk_fom
    from ufore_vedtak
    left join pen.t_vedtak v
        on
            ufore_vedtak.sak_id = v.sak_id
    group by ufore_vedtak.sak_id
),

legg_til_forste_virk as (
    select
        uv.*,
        fv.forste_virk_fom
    from ufore_vedtak uv
    left join forste_virk fv
        on uv.sak_id = fv.sak_id
)

select * from legg_til_forste_virk
