-- stg_t_sak

select
    sak_id,
    person_id,
    k_sak_t,
    dato_opprettet,
    opprettet_av,
    dato_endret,
    endret_av,
    versjon,
    k_sak_s,
    k_utlandstilknytning
from {{ source('pen', 't_sak') }}
