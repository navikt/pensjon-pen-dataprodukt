-- stg_t_kravhode

select
    sak_id,
    kravhode_id,
    kravhode_id_for,
    k_krav_gjelder,
    k_behandling_t,
    k_regelverk_t,
    k_afp_t,
    k_krav_s,
    opprettet_av,
    endret_av,
    pen_org_enhet_id,
    dato_opprettet,
    dato_endret,
    dato_onsket_virk,
    dato_mottatt_krav
from {{ source('pen', 't_kravhode') }}
