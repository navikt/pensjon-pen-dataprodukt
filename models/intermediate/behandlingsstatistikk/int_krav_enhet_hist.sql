-- int_krav_enhet_hist

with

ref_stg_t_krav_enhet_hist as (
    select * from {{ ref('stg_t_krav_enhet_hist') }}
),

ref_stg_t_pen_org_enhet as (
    select
        pen_org_enhet_id,
        org_enhet_id_fk
    from {{ ref('stg_t_pen_org_enhet') }}
),

join_org_enhet as (
    select
        hist.kravhode_id,
        hist.pen_org_enhet_id,
        hist.endret_av,
        hist.dato_opprettet,
        org_enhet.org_enhet_id_fk,
        lead(org_enhet.org_enhet_id_fk) over (
            partition by hist.kravhode_id
            order by hist.dato_opprettet asc
        ) as neste_org_enhet_id_fk
    from ref_stg_t_krav_enhet_hist hist
    left join ref_stg_t_pen_org_enhet org_enhet
        on hist.pen_org_enhet_id = org_enhet.pen_org_enhet_id
),

siste_enhet_sendt_til_klageenhet as (
    select
        kravhode_id,
        pen_org_enhet_id,
        org_enhet_id_fk,
        neste_org_enhet_id_fk,
        row_number() over (
            partition by kravhode_id
            order by dato_opprettet desc
        ) as rn
    from join_org_enhet
    where neste_org_enhet_id_fk = 4286
)

select
    kravhode_id,
    pen_org_enhet_id,
    org_enhet_id_fk,
    neste_org_enhet_id_fk
from siste_enhet_sendt_til_klageenhet
where rn = 1
