-- stg_t_uttaksgrad

select
    kravhode_id,
    sak_id,
    dato_virk_fom,
    dato_virk_tom,
    uttaksgrad
from {{ source('pen', 't_uttaksgrad') }}
