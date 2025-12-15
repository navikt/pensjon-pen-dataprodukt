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
        rtrim( -- fjerner bare siste komma
            max(case when k_kravlinje_t = 'UT' then 'UT,' else '' end)
            || max(case when k_kravlinje_t = 'BT' then 'BT,' else '' end)
            || max(case when k_kravlinje_t = 'ET' then 'ET,' else '' end)
            || max(case when k_kravlinje_t = 'ET' then 'ET,' else '' end)
            || max(case when k_kravlinje_t = 'TK' then 'TK,' else '' end)
            || max(case when k_kravlinje_t = 'UP' then 'UP,' else '' end)
            || max(case when k_kravlinje_t = 'GJR' then 'GJR,' else '' end)
            || max(case when k_kravlinje_t = 'ANKE' then 'ANKE,' else '' end)
            || max(case when k_kravlinje_t = 'KLAGE' then 'KLAGE,' else '' end)
            || max(case when k_kravlinje_t = 'UT_GJT' then 'UT_GJT,' else '' end)
            || max(case when k_kravlinje_t = 'FAST_UTG_INST' then 'FAST_UTG_INST,' else '' end),
            ','
        ) as kravlinjer
        -- listagg(k_kravlinje_t, ', ') within group (order by kravhode_id) as kravlinjer_listagg -- funker ikke pga order og BT-duplikater
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
