-- dataprodukt_ufore_diagnosekoder
-- gir alle saker som har diagnosekode fra snapshotet hvor radene er gyldige

select
    sak_id,
    hoveddiagnose,
    bidiagnoser,
    k_sak_t as sakstype,
    k_sak_s as sak_status,
    vedtak_id as vedtak_id_sist_lopende,
    k_vedtak_t as vedtakstype,
    k_vedtak_s as vedtak_status,
    dato_endret_vedtak as diagnose_oppdatert_tidspunkt
from {{ ref('snapshot_int_diagnosekode_lopende_ufore') }}
where -- dette fjerner duplikater, men da snapshotet har `hard_deletes: ignore` vil det også være rader som ikke lenger er løpende
    dbt_valid_to is null
    and dbt_valid_from <= sysdate
