-- stg_t_person_det

select
    person_grunnlag_id,
    k_sivilstand_t,
    rolle_fom,
    rolle_tom,
    bruk
from {{ source('pen', 't_person_det') }}
