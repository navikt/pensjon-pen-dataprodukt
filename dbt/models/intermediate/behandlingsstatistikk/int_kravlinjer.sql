-- int_kravlinjer
-- det ser ikke ut til at team sak har med noe på nivået kravlinjer og vilkårsvedtak
-- det er en mulighet for å utvide saksbehandlingsstatistikken med mer informasjon
-- kravhode -> kravlinjer -> vilkårsvedtak -> vilkårsvedtak resultat/begrunnelse
-- t_vilkars_vedtak_begr.vilkar_vedtak_id får t_vilkars_vedtak_begr.k_result_begr

-- i førsteomgang kobler denne modellen kun på normaliserte kravlinjer per kravhode


with

behandlinger as (
    select * from {{ ref('int_behandling') }}
),

ref_stg_t_kravlinje as (
    select * from {{ ref('stg_t_kravlinje') }}
),

kravlinjer_normalisert as (
    -- normaliserer kravlinjer per kravhode for å få kombinasjonene
    -- gir alle kravlinjer, ikke bare uføre
    select
        kravhode_id,
        listagg(distinct k_kravlinje_t, ',') within group (order by kravhode_id asc, k_kravlinje_t desc) as kravlinjer
    from ref_stg_t_kravlinje
    group by kravhode_id
),

behandlinger_kravlinjer as (
    -- joiner inn kravlinjer (normaliserte)
    select
        beh.*,
        krn.kravlinjer
    from behandlinger beh
    left join kravlinjer_normalisert krn
        on beh.kravhode_id = krn.kravhode_id
),

final as (
    select
        sak_id, -- kh
        kravhode_id, -- kh
        kravlinjer,
        k_krav_gjelder, -- kh
        k_krav_s, -- kh
        k_krav_arsak_t, -- ka
        k_behandling_t, -- kh
        opprettet_av, -- kh
        kravhode_id_for, -- kh
        dato_mottatt_krav, -- kh
        dato_opprettet, -- kh
        dato_onsket_virk -- kh
    from behandlinger_kravlinjer
)

select * from final
