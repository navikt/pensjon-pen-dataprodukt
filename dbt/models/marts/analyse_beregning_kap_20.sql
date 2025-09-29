{{
  config(
    materialized = 'table',
    )
}}

select * from {{ ref('int_beregning_kap_20') }}
