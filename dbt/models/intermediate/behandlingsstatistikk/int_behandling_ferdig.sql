-- int_behandling_ferdig
-- tar alt med kravstatus FERDIG og prøver å finne et behandlingsresultat
-- OBS! Ikke alle ferdige behandlinger er endelig ferdig, fordi vedtaket må være IVERKS for å gå til utbetaling
-- logikk for at vi kun sender IVERKS (og vedtak AVBR) til team sak kommer etter snapshot

with

ref_behandling as (
    select * from {{ ref('int_behandling') }}
    where k_krav_s = 'FERDIG'
),

ref_vedtak_ufore as (
    select * from {{ ref('stg_t_vedtak') }}
    where k_sak_t = 'UFOREP'
),

ref_t_vilkar_vedtak as (
    select * from {{ ref('stg_t_vilkar_vedtak') }}
),

ref_t_kravlinje as (
    select * from {{ ref('stg_t_kravlinje') }}
),

ref_t_k_kravlinje_t as (
    select * from {{ ref('stg_t_k_kravlinje_t') }}
),

vilkarsvedtak_hovedkravlinje as (
    select
        vv.vedtak_id,
        vv.dato_virk_fom,
        vv.k_vilkar_resul_t,
        kl.k_land_3_tegn_id
    from ref_t_vilkar_vedtak vv
    inner join ref_t_k_kravlinje_t tkl on vv.k_kravlinje_t = tkl.k_kravlinje_t
    left join ref_t_kravlinje kl on vv.kravlinje_id = kl.kravlinje_id
    where tkl.hoved_krav_linje = '1'
),

behandlinger_vedtak as (
-- en behandling kan ha flere vedtak
-- de kravhodene som har flere vedtak får duplikat her, men så fjernes den nedenfor i fjern_duplikater
    select
        beh.*,
        v.vedtak_id,
        v.k_vedtak_t,
        v.dato_vedtak,
        v.dato_virk_fom, -- utbetaltTid
        v.k_vedtak_s,
        v.k_vilkar_resul_t, -- resultat for hovedkravlinjen
        v.k_klageank_res_t,
        v.dato_endret as vedtak_dato_endret,
        case
            when substr(v.attesterer, 1, 1) in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
                then 'BRUKER-FNR' -- gjelder kun k_vedtak_t=OPPHOR og k_sak_t=UFOREP
            else v.attesterer
        end as attesterer
    from ref_behandling beh
    left join ref_vedtak_ufore v on beh.kravhode_id = v.kravhode_id
),

join_vilkar_vedtak as (
    -- for de fleste vedtakene vil v.k_vilkar_resul_t allerede være på behandlingene og brukes som resultat,
    -- men noen vedtak mangler dette, og da prøver vi å hente det fra vilkarsvedtak (kun hovedkravlinje)
    -- noen få vilkarsvedtak har to hovedkravlinjer, som også gir dupliker
    select
        bv.*,
        vv.k_land_3_tegn_id,
        vv.k_vilkar_resul_t as vv__k_vilkar_resul_t
    from behandlinger_vedtak bv
    left join vilkarsvedtak_hovedkravlinje vv
        on
            bv.vedtak_id = vv.vedtak_id
            and bv.dato_virk_fom = vv.dato_virk_fom
            and bv.k_vilkar_resul_t is null -- kun interessant med vv der denne er null
),

fjern_duplikater as (
    -- hånderer både duplikater fra vedtak og fra vilkarsvedtak
    select vv.*
    from (
        select
            bv.*,
            row_number() over (
                partition by bv.kravhode_id order by
                    -- bv.vedtak_dato_opprettet asc, -- velger det eldste vedtaket
                    bv.vedtak_id asc, -- lavest id er eldst. Dette håndterer at dato-opprettet kan være lik
                    (case when bv.k_land_3_tegn_id = '161' then 1 else 2 end) asc, -- velger norge på vilkårsvedtak, hvis norge er ett av landene
                    bv.k_land_3_tegn_id desc -- hvis norge ikke finnes, velges det vilkårsvedtaket med høyest landkode
            ) as rn
        from join_vilkar_vedtak bv
    ) vv
    where vv.rn = 1
),

sette_resultat as (
    select
        sak_id,
        kravhode_id,
        k_krav_gjelder,
        k_krav_s,
        k_krav_arsak_t,
        k_behandling_t,
        k_utlandstilknytning,
        ansvarlig_enhet,
        endret_av,
        opprettet_av,
        attesterer,
        dato_opprettet,
        greatest(dato_endret, vedtak_dato_endret) as dato_endret,
        dato_onsket_virk,
        dato_mottatt_krav,
        kravhode_id_for,
        vedtak_id,
        k_vedtak_t,
        dato_vedtak,
        dato_virk_fom,
        k_vedtak_s,
        k_vilkar_resul_t,
        vv__k_vilkar_resul_t,
        k_klageank_res_t
    from fjern_duplikater
)

select * from sette_resultat
