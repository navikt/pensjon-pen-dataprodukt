-- stg_t_beregning_info

select
    beregning_info_id,
    mottar_min_pensjonsniva,
    gjenlevrett_anv,
    inst_opph_anv,
    yrksk_grad,
    yrksk_anv,
    tt_anv,
    k_bereg_metode_t,
    tp_restpensjon,
    pt_restpensjon,
    gp_restpensjon
from {{ source('pen', 't_beregning_info') }}
