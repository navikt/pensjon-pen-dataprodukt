select * from {{ ref("stg_t_kravhode") }}
-- where dato_endret >= to_date({{ var('dato_endret_filter') }}, 'DD.MM.YYYY')
-- where dato_endret >= current_date - 14
where dato_endret >= {{ var("dato_endret_filter") }}
