-- stg_t_krav_arsak

select
    kravhode_id,
    k_krav_arsak_t
from {{ source('pen', 't_krav_arsak') }}
