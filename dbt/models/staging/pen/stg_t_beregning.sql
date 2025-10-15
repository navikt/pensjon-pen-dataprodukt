-- stg_t_beregning

select
    vedtak_id,
    beregning_id,
    total_vinner,
    dato_virk_fom,
    dato_virk_tom,
    k_minstepensj_t,
    k_minstepensj_arsak,
    k_bor_med_t,
    red_pga_inst_opph,
    brutto,
    netto,
    yug,
    tt_anv,
    k_bereg_metode_t
from {{ source('pen', 't_beregning') }}
