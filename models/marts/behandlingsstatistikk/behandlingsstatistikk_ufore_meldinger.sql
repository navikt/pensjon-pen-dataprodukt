-- behandlingsstatistikk_ufore_meldinger
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
        pot__tilbakek,
        periode_fom,
        periode_tom,
        kjoretidspunkt,

        -- BRUKER | SAKSBEH | SELVB_ANNET | SYSTEM | NULL, 
        {{ klassifiser_revisjonsfelt('endret_av') }} as endret_av_kode,
        {{ klassifiser_revisjonsfelt('opprettet_av') }} as opprettet_av_kode,
        {{ klassifiser_revisjonsfelt('attesterer') }} as attestert_av_kode
    from {{ ref('snapshot_int_ufore_behandling_grunnlag') }}
    -- {% if is_incremental() %}
    --     where kjoretidspunkt > (select coalesce(max(z.kjoretidspunkt), to_date('01.01.1900', 'DD.MM.YYYY')) from {{ this }} z)
    -- {% endif %}
),

sett_behandling_resultat_og_status as (
    select
        beh.*,
        case
            when beh.k_vedtak_s in ('IVERKS', 'STOPPET', 'STOPPES', 'REAK') and beh.k_krav_s = 'FERDIG' -- Vedtak med disse statusene er løpende
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
            when beh.k_krav_s = 'AVBRUTT' then beh.avbrutt_behandling_resultat
            when beh.k_krav_s = 'FERDIG' and beh.k_vedtak_s not in ('IVERKS', 'STOPPET', 'STOPPES', 'REAK', 'AVBR') then null
        end as behandling_resultat,
        case
            when beh.k_vedtak_s in ('IVERKS', 'STOPPET', 'STOPPES', 'REAK') and beh.k_krav_s = 'FERDIG' then 'FERDIG'
            when beh.k_vedtak_s = 'AVBR' and beh.k_krav_s = 'FERDIG' then 'AVBRUTT'
            when beh.k_krav_s = 'AVBRUTT' then 'AVBRUTT'
            when beh.k_krav_s = 'FERDIG' and beh.k_vedtak_s not in ('IVERKS', 'STOPPET', 'STOPPES', 'REAK', 'AVBR') then 'VENTER_VEDTAK'
            else beh.k_krav_s
        end as behandling_status,
        {{ beregn_behandling_metode(
            k_krav_s='beh.k_krav_s',
            ferdigbehandlet_tid='beh.ferdigbehandlet_tid',
            opprettet_av_kode='beh.opprettet_av_kode',
            attestert_av_kode='beh.attestert_av_kode',
            endret_av_kode='beh.endret_av_kode',
            k_krav_arsak_t='beh.k_krav_arsak_t',
            k_krav_gjelder='beh.k_krav_gjelder',
            k_behandling_t='beh.k_behandling_t'
        ) }} as behandling_metode
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
        case
            when behandling_resultat = 'VEDTAK_AVBRUTT' then trunc(dato_endret) -- disse flyttes fra FERDIG til AVBRUTT og må få ferdigbehandlet_tid satt
            when behandling_status in ('FERDIG', 'AVBRUTT') then ferdigbehandlet_tid  -- fjerner ferdigbehandlet_tid fra VENTER_VEDTAK
        end as ferdigbehandlet_tid, -- dato
        case -- utbetaltTid
            when {{ potensielt_lopende('k_krav_gjelder') }} = '1' then dato_virk_fom
            else null -- alle krav som ikke går til utbetaling, feks opphør, skal ikke ha utbetaltTid
        end as utbetalt_tid,
        cast(from_tz(cast(dato_endret as timestamp), 'Europe/Oslo') at time zone 'UTC' as timestamp(9)) as endret_tid,
        dato_onsket_virk as forventetoppstart_tid,
        kjoretidspunkt,
        k_sak_t as sak_ytelse,
        k_utlandstilknytning as sak_utland,
        k_krav_gjelder as behandling_type,
        behandling_status,
        behandling_resultat,
        behandling_metode,
        k_krav_arsak_t as behandling_arsak,
        opprettet_av,
        endret_av as saksbehandler,
        attesterer as ansvarlig_beslutter,
        ansvarlig_enhet,
        pot__tilbakek as tilbakekrev_belop,
        periode_fom as funksjonell_periode_fom,
        periode_tom as funksjonell_periode_tom,
        'PESYS' as fagsystem_navn,
        '1' as fagsystem_versjon,
        cast(systimestamp at time zone 'UTC' as timestamp(9)) as teknisk_tid -- brukes til last fra Oracle til BQ, vil skille seg fra kjoretidspunkt ved rekjøring
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
        kjoretidspunkt,
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
        fagsystem_versjon,
        teknisk_tid
    from nye_kolonnenavn
)

select * from final
