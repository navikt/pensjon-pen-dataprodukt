-- dataprodukt_alderspensjon_vedtak

{{
  config(
    materialized = 'incremental'
    )
}}

select
    vedtak_id,
    sak_id,
    kravhode_id,
    person_id,
    k_sak_t,
    k_vedtak_s,
    k_vedtak_t,
    dato_lopende_fom,
    dato_lopende_tom,
    k_regelverk_t,
    overgangsstonad_flagg,
    bostedsland,
    inntekt,
    inntekt_eps,
    eps_aarlig_inntekt,
    afp_privat_flagg,
    k_afp_t,
    k_sivilstand_t,

    {{ var("periode") }} as periode,
    sysdate as kjoretidspunkt
from {{ ref('int_vedtaksinfo') }}
where
    1 = 1
{% if is_incremental() %}
    and {{ var("periode") }} not in (select distinct periode from {{ this }}) -- noqa
{% endif %}
