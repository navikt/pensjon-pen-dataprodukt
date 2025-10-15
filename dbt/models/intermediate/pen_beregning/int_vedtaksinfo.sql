-- int_vedtaksinfo

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

ref_person as (
    select
        person_id,
        bostedsland
    from {{ ref('stg_t_person') }}
),

ref_person_grunnlag as (
    -- ett kravhode har 1-n persongrunnlag. 1 rad per person i kravhode, dvs mottaker, EPS, barn
    select
        person_grunnlag_id,
        kravhode_id,
        person_id
    from {{ ref('stg_t_person_grunnlag') }}
),

ref_person_det as (
    -- join via t_person_grunnlag
    select
        person_grunnlag_id,
        k_sivilstand_t,
        k_bor_med_t
    from {{ ref('stg_t_person_det') }}
    where
        bruk = 1
        and (
            rolle_fom < {{ periode_sluttdato(var("periode")) }}
            and (
                rolle_tom is null
                or rolle_tom >= {{ periode_sluttdato(var("periode")) }}
            )
        )
),

join_lopende_person as (
    select
        v.*,
        p.bostedsland
    from ref_int_lopende_vedtak_alder v
    left join ref_person p
        on v.person_id = p.person_id
),

join_lopende_person_grunnlag as (
    select
        v.*,
        pg.person_grunnlag_id
    from join_lopende_person v
    left join ref_person_grunnlag pg
        on
            v.kravhode_id = pg.kravhode_id
            and v.person_id = pg.person_id -- må også ha med person_id for å treffe personen i grunnlaget
),

join_lopende_person_det as (
    select
        v.*,
        pd.k_sivilstand_t,
        pd.k_bor_med_t
    from join_lopende_person_grunnlag v
    left join ref_person_det pd
        on v.person_grunnlag_id = pd.person_grunnlag_id
)

select
    vedtak_id,
    sak_id,
    kravhode_id,
    k_sak_t,
    k_vedtak_s,
    k_vedtak_t,
    dato_lopende_fom,
    dato_lopende_tom,
    k_regelverk_t,

    bostedsland,

    person_grunnlag_id,

    k_sivilstand_t,
    k_bor_med_t

from join_lopende_person_det
