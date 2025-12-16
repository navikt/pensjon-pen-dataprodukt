-- int_kravlinjer
-- det ser ikke ut til at team sak har med noe på nivået kravlinjer og vilkårsvedtak
-- det er en mulighet for å utvide saksbehandlingsstatistikken med mer informasjon
-- kravhode -> kravlinjer -> vilkårsvedtak -> vilkårsvedtak resultat/begrunnelse
-- t_vilkars_vedtak_begr.vilkar_vedtak_id får t_vilkars_vedtak_begr.k_result_begr

-- i førsteomgang kobler denne modellen kun på normaliserte kravlinjer per kravhode


with

behandlinger as (
    select * from pen_dataprodukt.int_behandling
    --select * from {{ ref('int_behandling') }}
),

kravlinjer_normalisert as (
    -- normaliserer kravlinjer per kravhode for å få kombinasjonene
    -- gir alle kravlinjer, ikke bare uføre
    select
        kravhode_id,
        listagg(distinct k_kravlinje_t, ',') within group (order by kravhode_id asc, k_kravlinje_t desc) as kravlinjer
    from pen.t_kravlinje
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
        vedtak_id, -- v
        kravlinjer,
        k_krav_gjelder, -- kh
        k_krav_s, -- kh
        k_krav_arsak_t, -- ka
        k_vedtak_t, -- v
        k_vilkar_resul_t, -- v
        k_sak_t, -- v
        k_behandling_t, -- kh
        dato_vedtak, -- v
        dato_virk_fom, -- v
        opprettet_av, -- kh
        kravhode_id_for, -- kh
        dato_mottatt_krav, -- kh
        dato_opprettet, -- kh
        dato_onsket_virk -- kh
    from behandlinger_kravlinjer
    order by
        sak_id desc,
        kravhode_id desc,
        dato_mottatt_krav desc,
        vedtak_id desc
)

select * from final
