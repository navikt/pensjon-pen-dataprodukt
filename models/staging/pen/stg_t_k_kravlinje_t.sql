-- stg_t_ytelse_komp

select
    k_kravlinje_t,
    hoved_krav_linje
from {{ source('pen', 't_k_kravlinje_t') }}
