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
        v.*,
        lag(v.dato_lopende_tom) over (partition by v.sak_id order by v.dato_lopende_fom asc) as forrige_dato_lopende_tom

    from ref_vedtak v
    where
        v.dato_lopende_fom is not null
),

vedtaksinfo as (
    select
        sak_id,
        max(
            case
                when dato_lopende_fom - forrige_dato_lopende_tom > 1
                then 1
                else 0
            end
        ) as har_ikke_sammenhengende_vedtak,
        min(dato_virk_fom) as forste_dato_virk_fom,
        max(dato_lopende_tom) as siste_dato_lopende_tom
    
    from vedtakshistorikk

    group by sak_id
)

select
    sak_id,
    har_ikke_sammenhengende_vedtak,
    forste_dato_virk_fom,
    siste_dato_lopende_tom
from vedtaksinfo
