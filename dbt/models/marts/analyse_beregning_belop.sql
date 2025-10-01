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
where
    1 = 1
{% if is_incremental() %}
    and {{ var("periode") }} not in (select distinct periode from {{ this }}) -- noqa
{% endif %}
