-- stonadsstatistikk_alder_vedtak

{{
  config(
    materialized = 'incremental',
    )
}}

with

ref_int_vedtaksinfo as (
    select * from {{ ref('int_vedtaksinfo') }}
),

ref_stg_t_person as (
    select
        person_id,
        fnr_fk
    from {{ ref('stg_t_person') }}
),

join_fnr as (
    select
        v.*,
        p.fnr_fk as fnr
    from ref_int_vedtaksinfo v
    left join ref_stg_t_person p on v.person_id = p.person_id
),

final as (
    select
        to_date({{ var("periode") }}, 'YYYYMM') as periode,
        sak_id,
        vedtak_id,
        kravhode_id,
        k_regelverk_t,
        person_id,
        fnr,
        k_sak_t,
        k_vedtak_s,
        k_vedtak_t,
        dato_lopende_fom,
        dato_lopende_tom,
        -- overgangsstonad_flagg, -- todo: fjerne utregning tidligere i løpet, men beholdt midlertidig for å kunne legge tilbake
        bostedsland,
        inntekt_fpi_belop as inntekt, -- endre navnet upstream
        inntekt_fpi_belop_eps as inntekt_eps, -- endre navnet upstream
        eps_aarlig_inntekt,
        afp_privat_flagg,
        k_afp_t,
        k_sivilstand_t,
        sysdate as kjoretidspunkt
    from join_fnr
    where
        1 = 1
    {% if is_incremental() %}
        and to_date({{ var("periode") }}, 'YYYYMM') not in (select distinct periode from {{ this }}) -- noqa
    {% endif %}
)

select * from final
