with

ref_behandling as (
    select *
    from {{ ref('int_behandling') }}
    where
        k_krav_s = 'FERDIG'
        and k_krav_gjelder not in ('KLAGE', 'ANKE')
),

ref_behandling_vedtak as (
    select * from {{ ref('int_behandling_vedtak') }}
),

ref_vilkar_vedtak as (
    select
        vv.*,
        kl.hoved_krav_linje
    from pen.t_vilkar_vedtak vv
    inner join pen.t_k_kravlinje_t kl
        on vv.k_kravlinje_t = kl.k_kravlinje_t
    where kl.hoved_krav_linje = '1'
),

behandlinger_vedtak as (
-- en behandling kan ha flere vedtak
    select
        beh.*,
        v.vedtak_id,
        v.k_sak_t, -- sakYtelse
        v.k_vedtak_t,
        v.dato_vedtak,
        v.dato_virk_fom, -- utbetaltTid
        v.k_vedtak_s, -- mulig deler av behandlingResultat (feks AVBR, men kan også være fra k_krav_s)
        v.k_vilkar_resul_t -- resultat for hovedkravlinjen
    from ref_behandling beh
    -- left join pen.t_vedtak v
    left join ref_behandling_vedtak v
        on
            beh.kravhode_id = v.kravhode_id
),

join_vilkar_vedtak as (
    select
        bv.*,
        vv.k_vilkar_resul_t as vv__k_vilkar_resul_t
    from behandlinger_vedtak bv
    left join ref_vilkar_vedtak vv
        on bv.vedtak_id = vv.vedtak_id

),

sette_resultat as (
    select
        sak_id,
        kravhode_id,
        k_krav_gjelder,
        k_krav_s,
        k_sak_s,
        k_krav_arsak_t,
        k_behandling_t,
        k_utlandstilknytning,
        opprettet_av,
        dato_opprettet,
        dato_onsket_virk,
        dato_mottatt_krav,
        kravhode_id_for,
        vedtak_id,
        k_sak_t,
        k_vedtak_t,
        dato_vedtak,
        dato_virk_fom,
        case when vedtak_id is not null then 'vedtak' end as vedtak,
        case
            when k_vilkar_resul_t is not null then k_vilkar_resul_t
            when k_krav_arsak_t = 'OPPHOR' then k_vedtak_s
            when vv__k_vilkar_resul_t is not null then vv__k_vilkar_resul_t
            when k_krav_gjelder = 'TILBAKEKR' then k_vedtak_s
            when k_krav_gjelder = 'OMGJ_TILBAKE' then k_vedtak_s
        end as behandling_resultat,
        k_vedtak_s,
        k_vilkar_resul_t
    from join_vilkar_vedtak
)

select * from sette_resultat

-- select
--     count(*) as ant,
--     behandling_resultat,
--     k_sak_s,
--     vedtak,
--     k_krav_arsak_t,
--     k_vedtak_t,
--     k_krav_gjelder
-- from sette_resultat
-- where behandling_resultat is null
-- group by
--     behandling_resultat,
--     vedtak,
--     k_sak_s,
--     k_krav_arsak_t,
--     k_vedtak_t,
--     k_krav_gjelder
-- order by
--     ant desc
-- 