-- behandlingsstatistikk_meldinger_v2
-- view som mapper over kolonnenavn fra pen til det team sak ønsker
-- setter også behandling_resultat og behandling_status
-- her endrer vi noen som er FERDIG til AVBRUTT basert på vedtaksstatus

{{
    config(
        materialized='incremental',
    )
}}

{%- set sekvensnummer_offset -%}
    {% if is_incremental() %}
    select nvl(max(z.sekvensnummer), 0) from {{ this }} z
    {% else %}
      select 0 from dual
    {% endif %}
{%- endset -%}
-- Henter største pk i targettabell eller 0 (tom liste)
-- Ved rekjøring etter første leveranse bør 0 endres til max(sekvensnumer), slik at team Sak ikke trenger å reversere indexen sin
-- kopi fra dvh-oppfolgin


with

ref_stg_t_person as (
    select
        person_id,
        fnr_fk
    from {{ ref('stg_t_person') }}
),

ref_stg_t_sak as (
    select
        sak_id,
        k_sak_t,
        person_id
    from {{ ref('stg_t_sak') }}
),

ref_behandlingsstatistikk_grunnlag as (
    select
        rownum + ({{ sekvensnummer_offset }}) as sekvensnummer,
        sak_id, -- kh
        kravhode_id, -- kh
        k_krav_gjelder, -- kh
        k_krav_s, -- kh
        k_krav_arsak_t, -- ka
        k_behandling_t, -- kh
        k_utlandstilknytning, -- sak
        ansvarlig_enhet, -- kh
        endret_av, -- kh
        opprettet_av, -- kh
        attesterer, -- vedtak
        dato_opprettet, -- kh
        dato_onsket_virk, -- kh
        dato_mottatt_krav, -- kh
        dato_virk_fom, -- v
        dato_endret, -- kh
        ferdigbehandlet_tid,
        kravhode_id_for, -- kh
        k_vedtak_t,
        k_vedtak_s,
        k_vilkar_resul_t,
        vv__k_vilkar_resul_t,
        k_klageank_res_t,
        avbrutt_behandling_resultat,
        kjoretidspunkt
    from {{ ref('snapshot_int_alder_behandling_grunnlag') }}
    {% if is_incremental() %}
        where kjoretidspunkt > (select coalesce(max(z.teknisk_tid), to_date('01.01.1900', 'DD.MM.YYYY')) from {{ this }} z)
    {% endif %}
),

sett_behandling_resultat_og_status as (
    select
        beh.*,
        case
            when beh.k_vedtak_s = 'IVERKS' and beh.k_krav_s = 'FERDIG'
                then
                    case
                        when beh.k_krav_gjelder in ('KLAGE', 'ANKE') then beh.k_klageank_res_t
                        when beh.k_vedtak_t in ('AVSL', 'OPPHOR') then beh.k_vedtak_t
                        when beh.k_vilkar_resul_t is not null then beh.k_vilkar_resul_t
                        when beh.vv__k_vilkar_resul_t is not null then beh.vv__k_vilkar_resul_t -- noen få rader mangler v.k_vilkar_resul_t
                        when beh.k_krav_gjelder in ('REGULERING', 'TILBAKEKR', 'OMGJ_TILBAKE', 'UTSEND_AVTALELAND') then 'INNV' -- disse mangler resultat på v og vv, så vedtak-status er beste vi har
                        else 'UKJENT_IVERKS'
                    end
            when beh.k_vedtak_s = 'AVBR' and beh.k_krav_s = 'FERDIG' then 'VEDTAK_AVBRUTT'
            when beh.k_vedtak_s in ('STOPPET', 'STOPPES') and beh.k_krav_s = 'FERDIG' then 'VEDTAK_STOPPET'
            when beh.k_krav_s = 'AVBRUTT' then beh.avbrutt_behandling_resultat
            when beh.k_krav_s = 'FERDIG' and beh.k_vedtak_s not in ('IVERKS', 'AVBR') then null
        end as behandling_resultat,
        case
            when beh.k_vedtak_s = 'IVERKS' and beh.k_krav_s = 'FERDIG' then 'FERDIG'
            when beh.k_vedtak_s in ('AVBR', 'STOPPET', 'STOPPES') and beh.k_krav_s = 'FERDIG' then 'AVBRUTT'
            when beh.k_krav_s = 'AVBRUTT' then 'AVBRUTT'
            when beh.k_krav_s = 'FERDIG' and beh.k_vedtak_s not in ('IVERKS', 'AVBR') then 'VENTER_VEDTAK'
            else beh.k_krav_s
        end as behandling_status
    from ref_behandlingsstatistikk_grunnlag beh
),

join_fnr as (
    select
        beh.*,
        s.k_sak_t,
        person.fnr_fk
    from sett_behandling_resultat_og_status beh
    left join ref_stg_t_sak s on beh.sak_id = s.sak_id
    left join ref_stg_t_person person on s.person_id = person.person_id
),

nye_kolonnenavn as (
    select
        sekvensnummer,
        cast(kravhode_id as varchar2(80)) as behandling_id,
        cast(kravhode_id_for as varchar2(80)) as relatertbehandling_id,
        'PESYS' as relatert_fagsystem,
        cast(sak_id as varchar2(80)) as sak_id,
        fnr_fk as aktor_id,
        dato_mottatt_krav as mottatt_tid,
        cast(from_tz(cast(dato_opprettet as timestamp), 'Europe/Oslo') at time zone 'UTC' as timestamp(9)) as registrert_tid,
        ferdigbehandlet_tid, -- dato
        case -- utbetaltTid
            when {{ potensielt_lopende('k_krav_gjelder') }} = '1' then dato_virk_fom
            else null -- alle krav som ikke går til utbetaling, feks opphør, skal ikke ha utbetaltTid
        end as utbetalt_tid,
        cast(from_tz(cast(dato_endret as timestamp), 'Europe/Oslo') at time zone 'UTC' as timestamp(9)) as endret_tid,
        dato_onsket_virk as forventetoppstart_tid,
        kjoretidspunkt as teknisk_tid,
        k_sak_t as sak_ytelse,
        k_utlandstilknytning as sak_utland,
        k_krav_gjelder as behandling_type,
        behandling_status,
        behandling_resultat,
        k_behandling_t as behandling_metode,
        k_krav_arsak_t as behandling_arsak,
        opprettet_av,
        endret_av as saksbehandler,
        attesterer as ansvarlig_beslutter,
        ansvarlig_enhet,
        -1.1 as tilbakekrev_belop,
        to_date('01.01.1900', 'DD.MM.YYYY') as funksjonell_periode_fom,
        to_date('01.01.1900', 'DD.MM.YYYY') as funksjonell_periode_tom,
        'PESYS' as fagsystem_navn,
        '1' as fagsystem_versjon
    from join_fnr
),

final as (
    select
        sekvensnummer,
        behandling_id,
        relatertbehandling_id,
        relatert_fagsystem,
        sak_id,
        aktor_id,
        mottatt_tid,
        registrert_tid,
        ferdigbehandlet_tid,
        utbetalt_tid,
        endret_tid,
        forventetoppstart_tid,
        teknisk_tid,
        sak_ytelse,
        sak_utland,
        behandling_type,
        behandling_status,
        behandling_resultat,
        behandling_metode,
        behandling_arsak,
        opprettet_av,
        saksbehandler,
        ansvarlig_beslutter,
        ansvarlig_enhet,
        tilbakekrev_belop,
        funksjonell_periode_fom,
        funksjonell_periode_tom,
        fagsystem_navn,
        fagsystem_versjon
    from nye_kolonnenavn
)

select * from final
