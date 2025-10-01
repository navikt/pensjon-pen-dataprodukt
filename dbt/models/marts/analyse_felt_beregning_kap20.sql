{% set cols_kap20_numeric = ["brutto", "netto", "uttaksgrad", "yrksk_grad", "tt_anv_g_opptj", "tt_anv_n_opptj"] %}
{% set cols_kap20_varchar = ["k_sak_t", "k_regelverk_t", "yrksk_anv", "gjenlevrett_anv", "inst_opph_anv", "mottar_min_pensjonsniva"] %}

{{
  config(
    tags=['testmodell'],
    materialized='view'
    )
}}


with

{% for col in cols_kap20_numeric %}
    stats_{{ col }} as (
        select
            '{{ col }}' as col_,
            'numeric' as type_,
            count(distinct {{ col }}) as distincts,
            count(case when {{ col }} is null then 1 end) as nulls_,
            count(case when {{ col }} = 0 then 1 end) as zeros,
            cast(median({{ col }}) as varchar2(20)) as median_,
            cast(max({{ col }}) as varchar2(20)) as max_,
            cast(min({{ col }}) as varchar2(20)) as min_
        from {{ ref('int_beregning_kap_20') }}
    ),
{% endfor %}

{% for col in cols_kap20_varchar %}
    stats_{{ col }} as (
        select
            '{{ col }}' as col_,
            'varchar2' as type_,
            count(distinct {{ col }}) as distincts,
            count(case when {{ col }} is null then 1 end) as nulls_,
            count(case when {{ col }} = '0' then 1 end) as zeros,
            cast(null as varchar2(20)) as median_,
            max({{ col }}) as max_,
            min({{ col }}) as min_
        from {{ ref('int_beregning_kap_20') }}
    ),
{% endfor %}

final as (
    {% for col in cols_kap20_numeric %}
        select * from stats_{{ col }}
        union all
    {% endfor %}
    {% for col in cols_kap20_varchar %}
        select * from stats_{{ col }}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select
    col_,
    type_,
    distincts,
    nulls_,
    zeros,
    max_,
    min_,
    median_
from final
