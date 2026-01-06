-- int_behandling
{{
  config(
    materialized = 'table',
    )
}}

with

ref_stg_t_sak as (
    select *
    from {{ ref('stg_t_sak') }}
),

ref_stg_t_krav_arsak as (
    select *
    from {{ ref('stg_t_krav_arsak') }}
),

ref_stg_t_kravhode as (
    select *
    from {{ ref('stg_t_kravhode') }}
),

ref_stg_t_pen_org_enhet as (
    select
        pen_org_enhet_id,
        org_enhet_id_fk
    from {{ ref('stg_t_pen_org_enhet') }}
),

behandlinger_kravhode as (
-- en sak kan ha flere behandlinger (kravhoder)
    select
        kh.kravhode_id, -- behandlingId
        kh.kravhode_id_for, -- relatertBehandlingId 
        kh.sak_id, -- sakId
        kh.dato_mottatt_krav, -- mottattTid
        kh.dato_opprettet, -- registrertTid
        kh.dato_onsket_virk, -- forventetOppstartTid
        kh.dato_endret,
        kh.k_krav_gjelder, -- behandlingType*
        kh.k_krav_s, -- behandlingStatus*
        case
            when substr(kh.opprettet_av, 1, 1) in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
                then 'BRUKER-FNR'
            else kh.opprettet_av
        end as opprettet_av, -- opprettetAv** med skjuling av fnr
        case
            when substr(kh.endret_av, 1, 1) in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
                then 'BRUKER-FNR'
            else kh.endret_av
        end as endret_av, -- saksbehandler**
        kh.pen_org_enhet_id,
        kh.k_behandling_t, -- metode
        s.k_sak_s,
        coalesce(s.k_utlandstilknytning, 'NASJONAL') as k_utlandstilknytning
    from ref_stg_t_kravhode kh
    inner join ref_stg_t_sak s
        on kh.sak_id = s.sak_id
    where s.k_sak_t = 'UFOREP'
),

behandlinger_kravarsak as (
    -- en kravarsak per kravhode
    select
        beh.*,
        ka.k_krav_arsak_t -- behandlingAarsak*
    from behandlinger_kravhode beh
    left join ref_stg_t_krav_arsak ka
        on beh.kravhode_id = ka.kravhode_id
),

-- joiner inn org_enhet_id_fk (fire siffer)
-- alternativt kan vi sende nåværende tilgang via pen.t_sak_tilgang, men krever da et datofilter
-- t_sak_tilgang har historikk og kan kalles "behandlende enhet" på et gitt tidspunkt
sette_org_enhet as (
    select
        beh.*,
        org.org_enhet_id_fk as ansvarlig_enhet
    from behandlinger_kravarsak beh
    left join ref_stg_t_pen_org_enhet org
        on beh.pen_org_enhet_id = org.pen_org_enhet_id
),

final as (
    select
        sak_id, -- kh
        kravhode_id, -- kh
        k_krav_gjelder, -- kh
        k_krav_s, -- kh
        k_sak_s, -- sak
        k_krav_arsak_t, -- ka
        k_behandling_t, -- kh
        k_utlandstilknytning, -- sak
        opprettet_av, -- kh
        endret_av, -- kh
        ansvarlig_enhet, -- pen_org_enhet
        dato_opprettet, -- kh
        dato_endret, -- kh
        dato_onsket_virk, -- kh
        dato_mottatt_krav, -- kh
        kravhode_id_for -- kh
    from sette_org_enhet
)

select * from final
