-- analyse_lopende_vedtak
-- henter ut uføresaker som er løpende per i dag

with

lopende_vedtak as (
    select
        v.vedtak_id,
        v.sak_id,
        v.kravhode_id,
        v.person_id,
        v.k_sak_t,
        v.dato_lopende_fom
    from {{ ref('stg_t_vedtak') }} v
    where
        v.k_sak_t = 'UFOREP'
        and v.dato_lopende_fom <= sysdate
        and (v.dato_lopende_tom is null or v.dato_lopende_tom >= trunc(sysdate))
),

aggregerer_kravlinjer_til_kravhoder as (
    -- sjekker om bruker har kravlinje BT og/eller UT_GJT
    -- dropper å telle opp FAST_UTG_INST (institusjonsopphold), som ikke er hovedkravlinje
    -- kravlinjer som UP blir behandlet i revurderinger og klager, men er ikke løpende vedtak
    select
        kh.kravhode_id,
        kh.k_krav_gjelder,
        kh.k_behandling_t,
        kh.k_krav_s,
        kh.dato_mottatt_krav,
        case when max(case when kl.k_kravlinje_t = 'BT' then 1 else 0 end) = 1 then 1 else 0 end as har_bt,
        case when max(case when kl.k_kravlinje_t = 'UT_GJT' then 1 else 0 end) = 1 then 1 else 0 end as har_gjt
        -- listagg(kl.k_kravlinje_t, ',') within group (order by kl.k_kravlinje_t desc) as kravlinjer
    from {{ ref('stg_t_kravhode') }} kh
    left join {{ ref('stg_t_kravlinje') }} kl
        on kh.kravhode_id = kl.kravhode_id
    group by
        kh.kravhode_id,
        kh.k_krav_gjelder,
        kh.k_behandling_t,
        kh.k_krav_s,
        kh.dato_mottatt_krav
),

join_kravhode as (
    select
        v.*,
        khkl.har_bt, -- barnetillegg
        khkl.har_gjt, -- gjenlevendetillegg
        khkl.k_krav_gjelder,
        khkl.k_behandling_t,
        khkl.k_krav_s,
        khkl.dato_mottatt_krav
    from lopende_vedtak v
    left join aggregerer_kravlinjer_til_kravhoder khkl
        on v.kravhode_id = khkl.kravhode_id
),

join_person as (
    select
        v.*,
        extract(year from p.dato_fodsel) as arskull,
        case when (p.bostedsland = 161 or p.bostedsland is null) then 1 else 0 end as bosatt_norge
    from join_kravhode v
    left join {{ ref('stg_t_person') }} p
        on v.person_id = p.person_id
),

join_persongrunnlag as (
    select
        v.*,
        pg.person_grunnlag_id
    from join_person v
    left join {{ ref('stg_t_person_grunnlag') }} pg
        on
            v.kravhode_id = pg.kravhode_id
            and v.person_id = pg.person_id
            -- må også ha med person_id for å treffe personen i grunnlaget (kun mottakeren)
)

select * from join_persongrunnlag
