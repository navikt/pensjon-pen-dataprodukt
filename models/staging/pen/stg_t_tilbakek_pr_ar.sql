select
    tilbakek_pr_ar_id,
    tilbakek_total_id
from {{ source('pen', 't_tilbakek_pr_ar') }}
