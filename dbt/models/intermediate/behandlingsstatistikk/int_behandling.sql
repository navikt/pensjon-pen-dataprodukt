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

behandlinger_kravhode as (
-- en sak kan ha flere behandlinger (kravhoder)
    select
        kh.kravhode_id, -- behandlingId
        kh.kravhode_id_for, -- relatertBehandlingId 
        kh.sak_id, -- sakId
        kh.dato_mottatt_krav, -- mottattTid
        kh.dato_opprettet, -- registrertTid
        kh.dato_onsket_virk, -- forventetOppstartTid
        kh.k_krav_gjelder, -- behandlingType*
        kh.k_krav_s, -- behandlingStatus*
        case
            when substr(kh.opprettet_av, 1, 1) in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
                then 'BRUKER-FNR'
            else kh.opprettet_av
        end as opprettet_av, -- opprettetAv** med skjuling av fnr
        kh.k_behandling_t -- metode
        -- kh.endret_av
    -- from pen.t_kravhode kh
    from {{ ref('stg_t_kravhode') }} kh
),

behandlinger_kravarsak as (
    -- en kravarsak per kravhode
    select
        beh.*,
        ka.k_krav_arsak_t -- behandlingAarsak*
    from behandlinger_kravhode beh
    -- left join pen.t_krav_arsak ka
    left join ref_stg_t_krav_arsak ka
        on beh.kravhode_id = ka.kravhode_id
),

behandlinger_sak as (
    select
        beh.*,
        s.k_sak_s,
        s.k_utlandstilknytning
    from behandlinger_kravarsak beh
    inner join ref_stg_t_sak s
        on beh.sak_id = s.sak_id
    where s.k_sak_t = 'UFOREP'
)

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
    dato_opprettet, -- kh
    dato_onsket_virk, -- kh
    dato_mottatt_krav, -- kh
    kravhode_id_for -- kh
from behandlinger_sak
