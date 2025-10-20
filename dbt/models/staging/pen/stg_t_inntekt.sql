-- stg_t_inntekt

select
    person_grunnlag_id,
    belop,
    dato_fom,
    dato_tom,
    k_inntekt_t,
    bruk
from {{ source('pen', 't_inntekt') }}
