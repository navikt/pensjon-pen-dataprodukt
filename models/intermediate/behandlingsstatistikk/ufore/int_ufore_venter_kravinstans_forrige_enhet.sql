-- int_ufore_venter_kravinstans_forrige_enhet
-- bruker historikken i snapshot_int_ufore_behandling_grunnlag til å finne
-- ansvarlig_enhet på raden rett før behandlingen fikk status VENTER_KRAVINSTANS

with

ref_snapshot as (
    select
        kravhode_id,
        k_krav_s,
        ansvarlig_enhet,
        dato_endret
    from {{ ref('snapshot_int_ufore_behandling_grunnlag') }}
),

ref_int_krav_enhet_hist as (
    select * from {{ ref('int_krav_enhet_hist') }}
),

med_forrige_enhet as (
    select
        kravhode_id,
        k_krav_s,
        ansvarlig_enhet,
        dato_endret,
        lag(ansvarlig_enhet) over (
            partition by kravhode_id
            order by dato_endret
        ) as forrige_ansvarlig_enhet
    from ref_snapshot
),

final as (
    select
        e.kravhode_id,
        e.k_krav_s,
        e.ansvarlig_enhet,
        coalesce(e.forrige_ansvarlig_enhet, hist.org_enhet_id_fk) as forrige_ansvarlig_enhet,
        e.dato_endret
    from med_forrige_enhet e
    left join ref_int_krav_enhet_hist hist
        on
            e.kravhode_id = hist.kravhode_id
            and e.forrige_ansvarlig_enhet is null
    where e.k_krav_s = 'VENTER_KLAGEINSTANS'
)

select * from final
