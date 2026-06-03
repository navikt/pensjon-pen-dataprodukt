-- int_diagnosekode_lopende_ufore
-- finner alle vedtak som er IVERKS og har en diagnosekode


-- bakgrunnsinfo:
-- t_vilkar har hoveddiagnose og bidiganoser
-- t_vilkar kobles til t_vilkar_vedtak, og vv har et gyldighetsintervall
-- t_vilkar_vedtak kobles til t_vedtak
-- t_vedtak kobles til t_sak
-- mulig vi kun trenger å bry oss om visse vedtakstyper, feks førstegangsvedtak og endringsvedtak

-- t_vilkar_vedtak har også person_id, men det er ikke alle vilkår som har IVERKS vedtak (avbrutt eller ikke ferdig utfylt)
-- vi bryr oss kun om diagnoser på vedtak som er IVERKS, fordi de er attesterte og ferdige

-- vi starter på løpende uførevedtak
-- deretter kobler vi på t_vilkar_vedtak og t_vilkar for å hente ut diagnoser
-- til slutt en snapshot oppå dette som kan gjøre all-time
-- hvis ting blir tregt kan vi begynne å kun se på vedtak IVERKS siste uke eller liknende

with

ref_vilkar as (
    select
        -- vilkar_id,
        vilkar_vedtak_id,
        -- k_vilkar_t,
        hoveddiagnose,
        bidiagnoser
    from {{ ref('stg_t_vilkar') }}
    where (hoveddiagnose is not null or bidiagnoser is not null)
    -- and k_vilkar_t = 'HENSIKTSMESSIG_BEH' -- usikkert om dette alltid gjelder
),

ref_vilkar_vedtak as (
    select
        vilkar_vedtak_id,
        vedtak_id,
        dato_virk_fom, -- brukes for å se på gyldighetsintervallet til vv, slik at vi ikke får duplikater
        dato_virk_tom
    from {{ ref('stg_t_vilkar_vedtak') }}
),

ref_vedtak as (
    select
        vedtak_id,
        sak_id,
        k_vedtak_t,
        k_vedtak_s,
        dato_endret,
        dato_lopende_fom,
        dato_lopende_tom
    from {{ ref('stg_t_vedtak') }}
    where k_sak_t = 'UFOREP' and k_vedtak_s = 'IVERKS'
),

ref_sak as (
    select
        sak_id,
        person_id,
        k_sak_s,
        k_sak_t
    from {{ ref('stg_t_sak') }}
    where k_sak_t = 'UFOREP'
),

lopende_iverksatte_vedtak_med_diagnose as (
    select
        v.sak_id,
        vil.hoveddiagnose,
        vil.bidiagnoser,
        s.k_sak_t,
        s.k_sak_s,
        v.vedtak_id,
        v.k_vedtak_t,
        v.k_vedtak_s,
        v.dato_endret as dato_endret_vedtak
    from ref_vedtak v
    -- her vil inner join fjerne vv og vil der vedtaket ikke er IVERKS
    inner join ref_vilkar_vedtak vv on v.vedtak_id = vv.vedtak_id
    inner join ref_vilkar vil on vv.vilkar_vedtak_id = vil.vilkar_vedtak_id
    inner join ref_sak s on v.sak_id = s.sak_id
    where
        v.k_vedtak_s = 'IVERKS'
        and v.dato_lopende_fom <= sysdate
        and (v.dato_lopende_tom is null or v.dato_lopende_tom >= sysdate)
        and vv.dato_virk_fom <= sysdate -- fordindrer duplikater på join mot vilkar_vedtak ved å se på nyeste
        and (vv.dato_virk_tom is null or vv.dato_virk_tom >= sysdate)
        and (vil.hoveddiagnose is not null or vil.bidiagnoser is not null)
),

final as (
    select
        sak_id,
        hoveddiagnose,
        bidiagnoser,
        k_sak_t,
        k_sak_s,
        vedtak_id,
        k_vedtak_t,
        k_vedtak_s,
        dato_endret_vedtak
    from lopende_iverksatte_vedtak_med_diagnose
)

select * from final
