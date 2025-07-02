-- stg_t_beregning_res

select
    beregning_res_id,
    vedtak_id,
    dato_virk_fom,
    dato_virk_tom,
    beregning_info_id,
    pen_under_utbet_id,
    beregning_info_avdod,
    ber_res_ap_2011_2016_id,
    ber_res_ap_2025_2016_id
from {{ source('pen', 't_beregning_res') }}
