-- stg_t_person_grunnlag

select
    person_grunnlag_id,
    kravhode_id,
    person_id
from {{ source('pen', 't_person_grunnlag') }}
