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
        k_afp_t,
        dato_lopende_fom,
        dato_lopende_tom
    from {{ ref('int_lopende_vedtak_alder') }}
),

ref_int_inntektsinfo as (
    select
        person_id,
        kravhode_id,
        inntekt_sfakpi_belop,
        inntekt_penskd_belop,
        inntekt_fki_belop,
        inntekt_fpi_belop,
        inntekt_pent_belop
    from {{ ref('int_inntektsinfo') }}
),

ref_int_eps_inntektsinfo as (
    select
        vedtak_id,
        inntekt_sfakpi_belop,
        inntekt_penskd_belop,
        inntekt_fki_belop,
        inntekt_fpi_belop,
        inntekt_pent_belop
    from {{ ref('int_eps_inntektsinfo') }}
),

ref_t_vilkar_vedtak as (
    select
        vedtak_id,
        k_vilk_vurd_t,
        dato_virk_fom,
        dato_virk_tom
    from {{ ref('stg_t_vilkar_vedtak') }}
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
        k_sivilstand_t
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

ref_vedtak as (
    select
        person_id,
        k_sak_t,
        dato_lopende_fom,
        dato_lopende_tom
    from {{ ref('stg_t_vedtak') }}
),

-- setter flagg for overgangsstønad via vilkår-vedtak
join_mange_til_en_vilkar_pa_vedtak as (
    -- joiner mot vilkår, og får da mange rader per vedtak
    select
        v.*,
        vilk.k_vilk_vurd_t
    from ref_int_lopende_vedtak_alder v
    left join ref_t_vilkar_vedtak vilk
        on
            v.vedtak_id = vilk.vedtak_id
            -- mulig dette datofilteret er redundant mtp join mot faktisk løpende vedtak
            and (vilk.dato_virk_tom is null or vilk.dato_virk_tom > {{ periode_sluttdato(var("periode")) }})
            and vilk.dato_virk_fom <= {{ periode_sluttdato(var("periode")) }}
),

-- group by på vedtak, så går tilbake til 1 rad per vedtak
setter_overgangsstonad_flagg as (
    select
        sak_id,
        person_id,
        vedtak_id,
        kravhode_id,
        k_sak_t,
        k_regelverk_t,
        k_vedtak_s,
        k_vedtak_t,
        k_afp_t,
        dato_lopende_fom,
        dato_lopende_tom,
        max(case
            when
                k_vilk_vurd_t in (
                    -- alle kodene under har dekode som slutter med " - Overgangsstønad"
                    'EGNE_BARN_FP',
                    'EGNE_BARN_GJP',
                    'EGNE_BARN_GJR',
                    'OMS_AVD_BARN_FP',
                    'OMS_AVD_BARN_GJP',
                    'OMS_AVD_BARN_GJR',
                    'OMSTILL_FP',
                    'OMSTILL_GJP',
                    'OMSTILL_GJR',
                    'UTDAN_FP',
                    'UTDAN_GJP',
                    'UTDAN_GJR'
                ) then 1
            else 0
        end) as overgangsstonad_flagg
    from join_mange_til_en_vilkar_pa_vedtak
    group by
        sak_id,
        person_id,
        vedtak_id,
        kravhode_id,
        k_sak_t,
        k_regelverk_t,
        k_vedtak_s,
        k_vedtak_t,
        k_afp_t,
        dato_lopende_fom,
        dato_lopende_tom
),

join_person_detaljer as (
    select
        v.*,
        p.bostedsland,
        pd.k_sivilstand_t
        -- her kan fnr legges til
    from setter_overgangsstonad_flagg v
    left join ref_person p
        on v.person_id = p.person_id
    left join ref_person_grunnlag pg
        on
            v.kravhode_id = pg.kravhode_id
            and v.person_id = pg.person_id -- må også ha med person_id for å treffe personen i grunnlaget
    left join ref_person_det pd
        on pg.person_grunnlag_id = pd.person_grunnlag_id
),

join_inntektsinfo as (
    select
        v.*,
        ii.inntekt_fpi_belop as inntekt,
        eii.inntekt_fpi_belop as inntekt_eps,
        case
            when eii.inntekt_sfakpi_belop > 0
                then eii.inntekt_sfakpi_belop

            else (
                coalesce(eii.inntekt_penskd_belop, 0)
                + coalesce(eii.inntekt_fki_belop, 0)
                + coalesce(eii.inntekt_fpi_belop, 0)
                + coalesce(eii.inntekt_pent_belop, 0)
            )
        end as eps_aarlig_inntekt
    from join_person_detaljer v
    left join ref_int_inntektsinfo ii
        on
            v.person_id = ii.person_id
            and v.kravhode_id = ii.kravhode_id
    left join ref_int_eps_inntektsinfo eii on v.vedtak_id = eii.vedtak_id
),

sett_afp_privat_flagg as (
    select
        v.*,
        case
            when afp_privat.k_sak_t = 'AFP_PRIVAT' then 1 else 0 -- kunne bare vært is not null
        end as afp_privat_flagg
    from join_inntektsinfo v
    left join ref_vedtak afp_privat
        on
            v.person_id = afp_privat.person_id
            and afp_privat.k_sak_t = 'AFP_PRIVAT'
            and afp_privat.dato_lopende_fom <= {{ periode_sluttdato(var("periode")) }}
            and (afp_privat.dato_lopende_tom is null or afp_privat.dato_lopende_tom >= trunc({{ periode_sluttdato(var("periode")) }}))
)

select
    vedtak_id,
    sak_id,
    kravhode_id,
    person_id,
    k_sak_t,
    k_vedtak_s,
    k_vedtak_t,
    dato_lopende_fom,
    dato_lopende_tom,
    k_regelverk_t,

    overgangsstonad_flagg,

    bostedsland,
    inntekt,
    inntekt_eps,
    eps_aarlig_inntekt,
    afp_privat_flagg,
    k_afp_t,

    k_sivilstand_t

from sett_afp_privat_flagg
