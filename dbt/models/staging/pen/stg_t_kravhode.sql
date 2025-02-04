-- stg_t_kravhode

select
    sak_id,
    kravhode_id,
    k_regelverk_t,
    k_afp_t
from {{ source('pen', 't_kravhode') }}
