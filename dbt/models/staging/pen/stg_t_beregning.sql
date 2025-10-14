-- stg_t_beregning

select
    vedtak_id,
    beregning_id,
    total_vinner,
    dato_virk_fom,
    dato_virk_tom,
    k_minstepensj_t,
    k_minstepensj_arsak,
    red_pga_inst_opph,
    brutto,
    netto,
    yug,
    tt_anv,
    k_bereg_metode_t
from {{ source('pen', 't_beregning') }}
