-- stg_t_beregning_info

select
    beregning_info_id,
    mottar_min_pensjonsniva,
    mottar_min_pensjniva_arsak,
    gjenlevrett_anv,
    rett_pa_gjlevenderett,
    inst_opph_anv,
    yrksk_grad,
    yrksk_anv,
    tt_anv,
    k_bereg_metode_t,
    tp_restpensjon,
    pt_restpensjon,
    gp_restpensjon
from {{ source('pen', 't_beregning_info') }}
