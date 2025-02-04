-- stg_t_beregning

select
    vedtak_id,
    beregning_id,
    total_vinner,
    dato_virk_fom,
    dato_virk_tom
from {{ source('pen', 't_beregning') }}
