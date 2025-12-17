select
    kravlinje_s_id,
    kravlinje_id,
    k_kravlinje_s,
    dato_opprettet,
    opprettet_av,
    dato_endret,
    endret_av,
    versjon
from {{ source('pen', 't_kravlinje_s') }}
