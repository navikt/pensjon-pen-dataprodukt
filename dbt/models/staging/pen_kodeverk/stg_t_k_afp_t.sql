-- stg_t_k_afp_t

select
    k_afp_t,
    dekode
from {{ source('pen', 't_k_afp_t') }}
