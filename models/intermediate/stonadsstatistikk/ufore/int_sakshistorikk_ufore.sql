with
ref_vedtak as (
    select
        sak_id,
        person_id,
        vedtak_id,
        kravhode_id,
        k_sak_t,
        k_vedtak_s,
        k_vedtak_t,
        dato_lopende_fom,
        dato_lopende_tom,
        dato_virk_fom
    from {{ ref('stg_t_vedtak') }}
    where
        k_sak_t = 'UFOREP'
),

vedtakshistorikk as (
    select
        v.sak_id,
        v.dato_lopende_fom,
        v.dato_lopende_tom,
        v.dato_virk_fom,
        lag(v.dato_lopende_tom) over (partition by v.sak_id order by v.dato_lopende_fom asc) as forrige_dato_lopende_tom

    from ref_vedtak v
    where
        v.dato_lopende_fom is not null
),

periodestart as (
    select
        vh.*,
        case
            when vh.forrige_dato_lopende_tom is null then 0
            when vh.dato_lopende_fom - vh.forrige_dato_lopende_tom > 1 then 1
            else 0
        end as ny_periode
    from vedtakshistorikk vh
),

perioder as (
    select
        ps.*,
        sum(ps.ny_periode) over (
            partition by ps.sak_id
            order by ps.dato_lopende_fom asc
            rows between unbounded preceding and current row
        ) + 1 as periode_nr
    from periodestart ps
),

sakshistorikk as (
    select
        sak_id,
        periode_nr,
        min(dato_lopende_fom) as forste_dato_lopende_fom,
        case
            when count(*) > count(dato_lopende_tom) then null
            else max(dato_lopende_tom)
        end as siste_dato_lopende_tom,
        min(dato_virk_fom) as forste_dato_virk_fom
    from perioder

    group by
        sak_id,
        periode_nr
),

sakshistorikk_info as (
    select
        sh.*,
        max(sh.periode_nr) over (partition by sh.sak_id) as totalt_antall_perioder,
        case
            when sh.periode_nr = max(sh.periode_nr) over (partition by sh.sak_id)
                then 1
            else 0
        end as er_siste_periode
    from sakshistorikk sh

)

select
    sak_id,
    totalt_antall_perioder,
    periode_nr,
    er_siste_periode,
    forste_dato_lopende_fom,
    siste_dato_lopende_tom,
    forste_dato_virk_fom

from sakshistorikk_info
