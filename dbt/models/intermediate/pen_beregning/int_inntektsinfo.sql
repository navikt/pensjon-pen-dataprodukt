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

join_person_info as (
    select
        pg.person_id,
        pg.kravhode_id,
        i.belop,
        i.dato_fom,
        i.k_inntekt_t --, it.k_inntekt_t, i.k_grunnlag_kilde 
    from ref_int_lopende_vedtak_alder v
    inner join ref_stg_t_person_grunnlag pg on v.kravhode_id = pg.kravhode_id
    inner join ref_stg_t_inntekt i
        on
            pg.person_grunnlag_id = i.person_grunnlag_id
            and i.bruk = 1
    where
        pg.person_id is not null
        and pg.kravhode_id is not null
        and i.bruk = 1
        and v.k_sak_t = 'ALDER'
        and v.k_vedtak_t in ('ENDRING', 'FORGANG', 'GOMR', 'SAMMENSTOT', 'OPPHOR', 'REGULERING')
        and v.k_vedtak_s in ('IVERKS', 'STOPPES', 'STOPPET', 'REAK')
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
        k_inntekt_t,
        last_value(belop) over (
            partition by person_id, kravhode_id, k_inntekt_t
            order by dato_fom rows between unbounded preceding and unbounded following
        ) as inntekt_belop
    from join_person_info
),

inntekt as (
    select
        inntekter.person_id,
        inntekter.kravhode_id,
        max(case when inntekter.k_inntekt_t = 'SFAKPI' then inntekter.inntekt_belop else 0 end) as inntekt_sfakpi_belop,
        max(case when inntekter.k_inntekt_t = 'PENSKD' then inntekter.inntekt_belop else 0 end) as inntekt_penskd_belop,
        max(case when inntekter.k_inntekt_t = 'FKI' then inntekter.inntekt_belop else 0 end) as inntekt_fki_belop,
        max(case when inntekter.k_inntekt_t = 'FPI' then inntekter.inntekt_belop else 0 end) as inntekt_fpi_belop,
        max(case when inntekter.k_inntekt_t = 'PENT' then inntekter.inntekt_belop else 0 end) as inntekt_pent_belop
    from velg_siste_belop_per_inntekt inntekter
    group by inntekter.person_id, inntekter.kravhode_id
)

select
    person_id,
    kravhode_id,
    inntekt_sfakpi_belop,
    inntekt_penskd_belop,
    inntekt_fki_belop,
    inntekt_fpi_belop,
    inntekt_pent_belop
from inntekt
