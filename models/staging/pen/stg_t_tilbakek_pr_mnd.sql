select
    tilbakek_pr_mnd_id,
    tilbakek_pr_ar_id,
    periode_fom,
    periode_tom,
    pot__tilbakek
from {{ source('pen', 't_tilbakek_pr_mnd') }}
