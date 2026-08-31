-- stg_t_krav_enhet_hist

select
    kravhode_id,
    pen_org_enhet_id,
    endret_av,
    dato_opprettet
from {{ source('pen', 't_krav_enhet_hist') }}
