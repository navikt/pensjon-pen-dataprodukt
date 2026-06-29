-- stonadsstatistikk_alder_vedtak
-- legg til kolonner forste_dato_virk_fom og forste_dato_lopende_fom, 

alter table pen_dataprodukt.stonadsstatistikk_alder_vedtak add (forste_dato_virk_fom date);
alter table pen_dataprodukt.stonadsstatistikk_alder_vedtak add (forste_dato_lopende_fom date);

alter table pen_dataprodukt.stonadsstatistikk_alder_vedtak modify (BOSTEDSLAND INVISIBLE , INNTEKT INVISIBLE , INNTEKT_EPS INVISIBLE , EPS_AARLIG_INNTEKT INVISIBLE , AFP_PRIVAT_FLAGG INVISIBLE , K_AFP_T INVISIBLE , K_SIVILSTAND_T INVISIBLE , KJORETIDSPUNKT INVISIBLE , PERIODE INVISIBLE)
alter table pen_dataprodukt.stonadsstatistikk_alder_vedtak modify (BOSTEDSLAND VISIBLE , INNTEKT VISIBLE , INNTEKT_EPS VISIBLE , EPS_AARLIG_INNTEKT VISIBLE , AFP_PRIVAT_FLAGG VISIBLE , K_AFP_T VISIBLE , K_SIVILSTAND_T VISIBLE , KJORETIDSPUNKT VISIBLE , PERIODE VISIBLE)