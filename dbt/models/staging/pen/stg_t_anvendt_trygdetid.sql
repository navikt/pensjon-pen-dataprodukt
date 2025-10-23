-- stg_t_vilkar_vedtak

select
    anvendt_trygdetid_id,
    pro_rata -- joines med brok_id
from {{ source('pen', 't_anvendt_trygdetid') }}
