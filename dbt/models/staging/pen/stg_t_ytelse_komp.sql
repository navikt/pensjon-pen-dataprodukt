-- stg_t_ytelse_komp

select
    ytelse_komp_id,
    k_ytelse_komp_t,
    k_minstepen_niva,
    min_pen_niva_id,
    ap_kap19_med_gjr,
    ap_kap19_uten_gjr,
    pen_under_utbet_id,
    beregning_id,
    f_min_pen_niva_id,
    anvendt_trygdetid,
    bruk,
    netto,
    fradrag,
    opphort
from {{ source('pen', 't_ytelse_komp') }}
