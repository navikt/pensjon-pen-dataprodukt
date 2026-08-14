select
    beregning_2011_id,
    beregning_res_kap_20,
    beholdninger_id
from {{ source('pen', 't_beregning_2011') }}
