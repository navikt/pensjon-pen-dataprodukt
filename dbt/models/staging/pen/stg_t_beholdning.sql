-- stg_t_beholdning

select
    beholdning_id,
    beholdninger_id,
    k_beholdning_t, -- PEN_B, AFP, GAR_PEN_B, GAR_T_B
    totalbelop,
    dato_fom,
    dato_tom
from {{ source('pen', 't_beholdning') }}
