-- stg_t_ytelse_komp

select
    k_ytelse_komp_t,
    pen_under_utbet_id,
    netto,
    bruk,
    ap_kap19_med_gjr,
    ap_kap19_uten_gjr,
    k_minstepen_niva,
    opphort
from {{ source('pen', 't_ytelse_komp') }}
