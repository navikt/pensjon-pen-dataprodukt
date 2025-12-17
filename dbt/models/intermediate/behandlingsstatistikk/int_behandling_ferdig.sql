with

ref_behandling as (
    select *
    from {{ ref('int_behandling') }}
    where k_krav_s = 'FERDIG'
),

ref_behandling_vedtak as (
    select * from {{ ref('int_behandling_vedtak') }}
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
)

select * from behandlinger_vedtak
