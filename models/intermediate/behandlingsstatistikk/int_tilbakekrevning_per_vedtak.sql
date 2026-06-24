-- int_tilbakekrevning_per_vedtak
-- aggregerer tilbakekrevningsdata fra t_tilbakek_pr_mnd per vedtak_id
-- brukes i join_tilbakekr i int_ufore_behandling_ferdig og int_alder_behandling_ferdig

with

ref_t_tilbakek_total as (
    select * from {{ ref('stg_t_tilbakek_total') }}
),

ref_t_tilbakek_pr_ar as (
    select * from {{ ref('stg_t_tilbakek_pr_ar') }}
),

ref_t_tilbakek_pr_mnd as (
    select * from {{ ref('stg_t_tilbakek_pr_mnd') }}
)

select
    ref_t_tilbakek_total.vedtak_id,
    min(ref_t_tilbakek_pr_mnd.periode_fom) as periode_fom,
    max(ref_t_tilbakek_pr_mnd.periode_tom) as periode_tom,
    sum(ref_t_tilbakek_pr_mnd.pot__tilbakek) as pot__tilbakek
from ref_t_tilbakek_total
inner join ref_t_tilbakek_pr_ar
    on ref_t_tilbakek_total.tilbakek_total_id = ref_t_tilbakek_pr_ar.tilbakek_total_id
inner join ref_t_tilbakek_pr_mnd
    on ref_t_tilbakek_pr_ar.tilbakek_pr_ar_id = ref_t_tilbakek_pr_mnd.tilbakek_pr_ar_id
where ref_t_tilbakek_pr_mnd.pot__tilbakek > 0
group by
    ref_t_tilbakek_total.vedtak_id
