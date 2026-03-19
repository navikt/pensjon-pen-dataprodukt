with

ref_stg_t_kravhode as (
    select
        kravhode_id,
        sak_id,
        k_krav_s,
        dato_opprettet
    from {{ ref('stg_t_kravhode') }}
    where dato_opprettet < {{ var('behandlingsstatistikk_start_dato') }}
),

ref_stg_t_vedtak as (
    select
        vedtak_id,
        kravhode_id,
        k_vedtak_t,
        k_vedtak_s
    from {{ ref('stg_t_vedtak') }}
),

ref_stg_t_sak as (
    select
        sak_id,
        k_sak_t
    from {{ ref('stg_t_sak') }}
),

join_vedtak_og_sak as (
    select
        kh.sak_id,
        kh.kravhode_id,
        kh.k_krav_s,
        v.k_vedtak_s,
        kh.dato_opprettet
    from ref_stg_t_kravhode kh
    inner join ref_stg_t_sak s on kh.sak_id = s.sak_id
    left join ref_stg_t_vedtak v
        on
            kh.kravhode_id = v.kravhode_id
            and v.k_vedtak_t != 'REGULERING'

    where s.k_sak_t = 'UFOREP'
),

apne_behandlinger as (
    select
        sak_id,
        kravhode_id,
        k_vedtak_s,
        k_krav_s,
        dato_opprettet
    from join_vedtak_og_sak
    where
        k_krav_s not in ('FERDIG', 'AVBRUTT')
        or (k_vedtak_s != 'IVERKS' and k_krav_s = 'FERDIG')
)

select * from apne_behandlinger
