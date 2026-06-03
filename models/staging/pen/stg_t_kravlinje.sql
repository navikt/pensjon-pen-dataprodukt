select
    kravlinje_id,
    person_id,
    kravhode_id,
    k_land_3_tegn_id,
    k_kravlinje_t,
    kravlinje_s_id,
    dato_opprettet,
    opprettet_av,
    dato_endret,
    endret_av,
    versjon,
    k_utland_s
from {{ source('pen', 't_kravlinje') }}
