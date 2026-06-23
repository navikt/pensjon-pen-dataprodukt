select
    tilbakek_total_id,
    vedtak_id,
    ber_skyldig_netto,
    oppg_skyldig_netto
from {{ source('pen', 't_tilbakek_total') }}
