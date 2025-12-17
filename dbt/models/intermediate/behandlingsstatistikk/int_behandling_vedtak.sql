with

ref_stg_t_vedtak as (
    select *
    from {{ ref('stg_t_vedtak') }}
    where
        k_sak_t = 'UFOREP'
        and k_vedtak_t != 'REGULERING'
),

-- I 2024 og 2025 er det kun 300 rader der et krav har fler enn 1 vedtak.
-- Vi anntar at det første vedtaket er riktigst i disse tilfellene.
aggreger_krav as (
    select * from
        (select
            v.*,
            row_number() over (partition by v.kravhode_id order by v.vedtak_id asc) as rn
        from ref_stg_t_vedtak v)
    where rn = 1
    --group by kravhode_id
)

select * from aggreger_krav
