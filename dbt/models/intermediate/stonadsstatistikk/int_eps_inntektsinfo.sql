with

ref_int_lopende_vedtak_alder as (
    select
        sak_id,
        person_id,
        vedtak_id,
        kravhode_id,
        k_sak_t,
        k_regelverk_t,
        k_vedtak_s,
        k_vedtak_t,
        dato_lopende_fom,
        dato_lopende_tom
    from {{ ref('int_lopende_vedtak_alder') }}
),

ref_stg_t_inntekt as (
    select
        person_grunnlag_id,
        belop,
        dato_fom,
        dato_tom,
        k_inntekt_t,
        bruk
    from {{ ref('stg_t_inntekt') }}
),

ref_stg_t_person_grunnlag as (
    select
        person_grunnlag_id,
        kravhode_id,
        person_id
    from {{ ref('stg_t_person_grunnlag') }}
),

ref_stg_t_person_det as (
    select
        person_grunnlag_id,
        k_bor_med_t,
        dato_fom,
        dato_tom,
        bruk
    from {{ ref('stg_t_person_det') }}
),

join_person_info as (
    select
        pg.person_id,
        pg.kravhode_id,
        v.vedtak_id,
        i.belop,
        i.dato_fom,
        i.dato_tom,
        i.k_inntekt_t
    from ref_int_lopende_vedtak_alder v
    inner join ref_stg_t_person_grunnlag pg on v.kravhode_id = pg.kravhode_id
    inner join ref_stg_t_person_det pd
        on
            pg.person_grunnlag_id = pd.person_grunnlag_id
            and pd.dato_fom <= current_date
            and (pd.dato_tom is null or pd.dato_tom >= current_date)
            and pd.bruk = 1
    inner join ref_stg_t_inntekt i
        on pg.person_grunnlag_id = i.person_grunnlag_id
    where
        pg.person_id is not null
        and pg.kravhode_id is not null
        and i.bruk = 1
        and pd.k_bor_med_t in ('J_EKTEF', 'N_GIFT', 'SAMBOER1_5', 'SAMBOER3_2', 'J_PARTNER', 'N_GIFT_P')
        and v.k_sak_t = 'ALDER'
        and to_char(i.dato_fom, 'YYYYMM') <= to_char(to_date(current_date), 'YYYYMM')
        and (
            i.dato_tom is null
            or to_char(i.dato_tom, 'YYYYMM') >= to_char(current_date, 'YYYYMM')
        )
),

velg_siste_belop_per_inntekt as (
    select distinct
        person_id,
        kravhode_id,
        vedtak_id,
        k_inntekt_t,
        last_value(belop) over (
            partition by person_id, kravhode_id, vedtak_id, k_inntekt_t
            order by dato_fom rows between unbounded preceding and unbounded following
        ) as inntekt_belop
    from join_person_info
),

inntekt_eps as (
    select
        vedtak_id,
        max(case when k_inntekt_t = 'SFAKPI' then inntekt_belop else 0 end) as inntekt_sfakpi_belop,
        max(case when k_inntekt_t = 'PENSKD' then inntekt_belop else 0 end) as inntekt_penskd_belop,
        max(case when k_inntekt_t = 'FKI' then inntekt_belop else 0 end) as inntekt_fki_belop,
        max(case when k_inntekt_t = 'FPI' then inntekt_belop else 0 end) as inntekt_fpi_belop,
        max(case when k_inntekt_t = 'PENT' then inntekt_belop else 0 end) as inntekt_pent_belop
    from velg_siste_belop_per_inntekt
    group by vedtak_id
)

select
    vedtak_id,
    inntekt_sfakpi_belop,
    inntekt_penskd_belop,
    inntekt_fki_belop,
    inntekt_fpi_belop,
    inntekt_pent_belop
from inntekt_eps
