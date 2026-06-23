select
    tilbakek_pr_mnd_id,
    tilbakek_pr_ar_id,
    periode_fom,
    periode_tom,
    pot_feilutb
from {{ source('pen', 't_tilbakek_pr_mnd') }}
