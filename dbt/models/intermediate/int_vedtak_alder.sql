-- int_vedtak_alder

with

ref_vedtak as (
    select
        vedtak_id,
        sak_id,
        kravhode_id,
        k_sak_t,
        dato_virk_fom,
        dato_virk_tom,
        dato_lopende_fom,
        dato_lopende_tom
    from {{ ref('stg_t_vedtak') }}
    where k_sak_t = 'ALDER'
),

ref_kravhode as (
    select
        sak_id,
        kravhode_id,
        k_regelverk_t,
        k_krav_gjelder,
        k_afp_t
    from {{ ref('stg_t_kravhode') }}
),

joinet as (
    select
        v.*
        , kh.k_regelverk_t
        , kh.k_krav_gjelder
        , kh.k_afp_t
    from ref_vedtak v
    left join ref_kravhode kh on v.kravhode_id = kh.kravhode_id
),

final as (
    select
        vedtak_id,
        sak_id,
        kravhode_id,
        k_sak_t,
        dato_lopende_fom,
        dato_lopende_tom,
        dato_virk_tom,
        dato_virk_fom,
        -- kravhode
        k_regelverk_t,
        k_krav_gjelder,
        k_afp_t
    from joinet
)

select * from joinet
