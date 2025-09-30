{{
  config(
    materialized = 'incremental',
    )
}}

select
    bb.*,
    {{ var("periode") }} as periode,
    sysdate as kjoretidspunkt
from {{ ref('int_beregning_belop') }} bb
