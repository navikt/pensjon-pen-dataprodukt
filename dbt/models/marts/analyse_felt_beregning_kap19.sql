{% set cols_kap19_numeric = ["brutto", "netto", "yug", "tt_anv"] %}
{% set cols_kap19_varchar = ["k_sak_t", "k_regelverk_t", "red_pga_inst_opph", "k_minstepensj_t"] %}

{{
  config(
    tags=['testmodell'],
    materialized='view'
    )
}}


with

{% for col in cols_kap19_numeric %}
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
        from {{ ref('int_beregning_kap_19') }}
    ),
{% endfor %}

{% for col in cols_kap19_varchar %}
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
        from {{ ref('int_beregning_kap_19') }}
    ),
{% endfor %}

final as (
    {% for col in cols_kap19_numeric %}
        select * from stats_{{ col }}
        union all
    {% endfor %}
    {% for col in cols_kap19_varchar %}
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
