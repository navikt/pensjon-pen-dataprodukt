-- stg_t_person

select
    fnr_fk,
    person_id,
    bostedsland
from {{ source('pen', 't_person') }}
