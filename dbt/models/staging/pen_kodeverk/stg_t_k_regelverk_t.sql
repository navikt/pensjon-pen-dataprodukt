-- stg_t_k_regelverk_t

select
    k_regelverk_t,
    dekode
from {{ source('pen', 't_k_regelverk_t') }}
