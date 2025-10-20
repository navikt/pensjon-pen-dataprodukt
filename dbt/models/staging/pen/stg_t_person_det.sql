-- stg_t_person_det

select
    person_grunnlag_id,
    k_sivilstand_t,
    k_bor_med_t,
    rolle_fom,
    rolle_tom,
    dato_fom,
    dato_tom,
    bruk
from {{ source('pen', 't_person_det') }}
