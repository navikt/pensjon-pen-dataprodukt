-- stg_t_ytelse_komp

select
    ytelse_komp_id,
    k_ytelse_komp_t,
    k_minstepen_niva,
    ap_kap19_med_gjr,
    ap_kap19_uten_gjr,
    pen_under_utbet_id,
    beregning_id,
    bruk,
    netto,
    opphort
from {{ source('pen', 't_ytelse_komp') }}
