select
    tilbakek_total_id,
    vedtak_id
from {{ source('pen', 't_tilbakek_total') }}
