-- migration: rename teknisk_tid to kjoretidspunkt in behandlingsstatistikk mart tables
-- reason: kolonnenavnet kjoretidspunkt brukes i kildedata og modellene, og skal reflekteres i mart-tabellene

-- behandlingsstatistikk_alder_meldinger
alter table behandlingsstatistikk_alder_meldinger rename column teknisk_tid to kjoretidspunkt;
alter table behandlingsstatistikk_alder_meldinger add (teknisk_tid timestamp default systimestamp at time zone 'UTC');
update behandlingsstatistikk_alder_meldinger set teknisk_tid = kjoretidspunkt;
commit;


-- behandlingsstatistikk_ufore_meldinger
alter table behandlingsstatistikk_ufore_meldinger rename column teknisk_tid to kjoretidspunkt;
alter table behandlingsstatistikk_ufore_meldinger add (teknisk_tid timestamp default systimestamp at time zone 'UTC');
update behandlingsstatistikk_ufore_meldinger set teknisk_tid = kjoretidspunkt;
commit;


-- bigquery equivalent
-- alter table `pensjon-saksbehandli-prod-1f83.pen_dataprodukt_dataset.saksbehandlingsstatistikk_ufore`
--     alter column teknisk_tid set default current_timestamp();
-- alter table `pensjon-saksbehandli-prod-1f83.pen_dataprodukt_dataset.saksbehandlingsstatistikk_alder`
--     alter column teknisk_tid set default current_timestamp();
-- 
-- alter table `pensjon-saksbehandli-dev-cb76.pen_dataprodukt_dataset.saksbehandlingsstatistikk_ufore`
--     alter column teknisk_tid set default current_timestamp();
-- alter table `pensjon-saksbehandli-dev-cb76.pen_dataprodukt_dataset.saksbehandlingsstatistikk_alder`
--     alter column teknisk_tid set default current_timestamp();
