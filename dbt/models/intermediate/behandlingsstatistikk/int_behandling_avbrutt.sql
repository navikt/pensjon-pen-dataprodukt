-- int_behandling_avbrutt
-- mål med modellen er å koble inn k_kravlinje_s for å si noe om begrunnelse for avbrutt behandling
-- resultatet er enten FEILREGISTRERT, HENLAGT eller TRUKKET

with

ref_behandling as (
    select * from {{ ref('int_behandling') }}
    where k_krav_s = 'AVBRUTT'
),

ref_kravlinje as (
    select
        kravhode_id,
        kravlinje_s_id
    from {{ ref('stg_t_kravlinje') }}
),

ref_kravlinje_s as (
    select
        kravlinje_s_id,
        k_kravlinje_s
    from {{ ref('stg_t_kravlinje_s') }}
),

-- aggregerer opp kravlinje-status med prioritert rekkefølge:
-- trukket > henlagt > feilregistrert > annet (typ 8 krav all time)
status_aggregert as (
    select
        kl.kravhode_id,
        case
            when max(case when kls.k_kravlinje_s = 'TRUKKET' then 1 else 0 end) = 1 then 'TRUKKET'
            when max(case when kls.k_kravlinje_s = 'HENLAGT' then 1 else 0 end) = 1 then 'HENLAGT'
            when max(case when kls.k_kravlinje_s = 'FEILREGISTRERT' then 1 else 0 end) = 1 then 'FEILREGISTRERT'
            else 'FEILREGISTRERT'
        end as behandling_resultat
    from ref_kravlinje kl
    left join ref_kravlinje_s kls
        on kl.kravlinje_s_id = kls.kravlinje_s_id
    group by kl.kravhode_id
),

-- joiner inn k_kravlinje_s til avbrutte behandlinger
behandlinger_avbrutt_med_status as (
    select
        beh.*,
        status_aggregert.behandling_resultat
    from ref_behandling beh
    left join status_aggregert
        on beh.kravhode_id = status_aggregert.kravhode_id
),

final as (
    select
        behandling_resultat, -- kls aggregert på kravhode med prioritert rekkefølge
        sak_id, -- kh
        kravhode_id, -- kh
        k_krav_gjelder, -- kh
        k_krav_s, -- kh
        k_krav_arsak_t, -- ka
        k_behandling_t, -- kh
        k_utlandstilknytning, -- sak
        ansvarlig_enhet,
        endret_av,
        opprettet_av, -- kh
        dato_opprettet, -- kh
        dato_endret, -- kh
        dato_onsket_virk, -- kh
        dato_mottatt_krav, -- kh
        kravhode_id_for -- kh
    from behandlinger_avbrutt_med_status
)

select * from final
