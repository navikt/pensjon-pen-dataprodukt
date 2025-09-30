{{
  config(
    materialized = 'incremental',
    )
}}

select
    b.*,
    {{ var("periode") }} as periode,
    sysdate as kjoretidspunkt
from {{ ref('int_beregning') }} b
