-- stg_t_beholdning.sql

select
    beholdning_id,
    beregning_info_id,
    k_beholdning_t, -- PEN_B, AFP, GAR_PEN_B, GAR_T_B
    totalbelop
from {{ source('pen', 't_beholdning') }}
