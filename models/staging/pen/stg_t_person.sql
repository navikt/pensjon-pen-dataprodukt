-- stg_t_person

select
    fnr_fk,
    person_id,
    dato_fodsel,
    bostedsland
from {{ source('pen', 't_person') }}
