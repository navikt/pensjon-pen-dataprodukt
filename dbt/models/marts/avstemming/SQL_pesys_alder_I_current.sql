
--create or replace view vw_pesys_alder_i_current as
-------------------------------------------------------------------------------
-- Eirik Grønli
-- 29. januar 2024
--
-- Endret: 02 des 2024
--         Gjenlevendeytelse kapittel 19
-- Endret: 30 apr 2025
--         Innvilget gjenlevenderett.
--         Kommunal ytelse.
-- Endret: 30 sep 2025
--         Fjernet join til K-tabellene T_K_AFP_T og t_k_regelverk_t.
--         Erstattet dem med case-uttrykk.
--         Tok vekk join til t_uttaksgrad i CTE yk_bres.
--         Tok vekk personnr og join mot t_person.
--         Tok bort join til pen_under_utbet i aktive (union all fra yk_bres).
-- Endret: 10 okt 2025
--         La inn sjekk av virkningsdatoer for beregning_info for evt. avdød,
--         men er kommentert vekk inntil videre.
--         Skreller bort noen unødvendige felter, og litt småpuss av koden.
-------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- Starter først med definisjoner av høvelige CTEs (Common Table Expressions).
------------------------------------------------------------------------------
with 

-------------------------------------------------------------------------------------------
-- CTE for overgangsstønad.
-- Den oppretter en CTE på basis av data fra en statisk fil som benyttes i dagens mapping.
-------------------------------------------------------------------------------------------
overgangsstonad as
( select id, 
         rtrim(ltrim(max(case when id2 = 1 then splitted_element else null end))) kode, 
         max(case when id2 = 5 then splitted_element else null end) flagg
    from (select id, 
                 level as id2,
                 REGEXP_SUBSTR(tekst, '[^;]+', 1, LEVEL) AS splitted_element
            from (      select  1  as id, ';INNV1;189;Innvilget § 17-10.1a;0;1;1;   ;;;;;;;;' as tekst from dual
                  union select  2  as id, ';INNV2;189;Innvilget § 17-10.1b;0;1;1;   ;;;;;;;;' as tekst from dual
                  union select  3  as id, ';INNV3;189;Innvilget § 17-10.1c;0;1;1;   ;;;;;;;;' as tekst from dual
                  union select  4  as id, ';INNV4;189;Innvilget § 17-10.2;0;1;1;   ;;;;;;;;' as tekst from dual
                  union select  5  as id, ';INNV5;189;Innvilget gammel § 10-6 (før 1989);0;1;1;   ;;;;;;;;' as tekst from dual
                  union select  6  as id, ';EGNE_BARN_GJP;189;Egne barn - Overgangsstønad;1;0;1;   ;;;;;;;;' as tekst from dual
                  union select  7  as id, ';EGNE_BARN_KUN_GJP;189;Egne barn;0;1;1;   ;;;;;;;;' as tekst from dual
                  union select  8  as id, ';UTDAN_GJP;189;Nødvendig utdanning - Overgangsstønad;1;0;1;   ;;;;;;;;' as tekst from dual
                  union select  9  as id, ';;OMS_AVD_BARN_GJP;189;Omsorg avdødes barn - Overgangsstønad;1;0;1;   ;;;;;;;;' as tekst from dual
                  union select 10  as id, ';OMS_AVD_BARN_KUN_GJP;189;Omsorg avdødes barn;0;1;1;   ;;;;;;;;' as tekst from dual
                  union select 11  as id, ';OMSTILL_GJP;189;Omstillingsperiode - Overgangsstønad;1;0;1;   ;;;;;;;;' as tekst from dual
                  union select 12  as id, ';EGNE_BARN_FP;188;Egne barn - Overgangsstønad;1;0;1;   ;;;;;;;;' as tekst from dual
                  union select 13  as id, ';EGNE_BARN_KUN_FP;188;Egne barn;0;1;1;   ;;;;;;;;' as tekst from dual
                  union select 14  as id, ';UTDAN_FP;188;Nødvendig utdanning - Overgangsstønad;1;0;1;   ;;;;;;;;' as tekst from dual
                  union select 15  as id, ';OMS_AVD_BARN_FP;188;Omsorg avdødes barn - Overgangsstønad;1;0;1;' as tekst from dual   
                  union select 16  as id, ';OMS_AVD_BARN_KUN_FP;188;Omsorg avdødes barn;0;1;1;' as tekst from dual   
                  union select 17  as id, ';OMSTILL_FP;188;Omstillingsperiode - Overgangsstønad;1;0;1;' as tekst from dual   
                  union select 18  as id, ';UNDER_UTDAN;185;Under utdanning;1;0;1;' as tekst from dual   
                  union select 19  as id, ';PRAKTIK;185;Lærling/praktikant;1;0;1;' as tekst from dual   
                  union select 20  as id, ';INNV1_GJR;194;Innvilget §17-10.1a;0;1;1;' as tekst from dual   
                  union select 21  as id, ';INNV2_GJR;194;Innvilget §17-10.1b;0;1;1;' as tekst from dual   
                  union select 22  as id, ';INNV3_GJR;194;Innvilget § 17-10.1c;0;1;1;' as tekst from dual   
                  union select 23  as id, ';INNV4_GJR;194;Innvilget § 17- 10.2;0;1;1;' as tekst from dual   
                  union select 24  as id, ';INNV5_GJR;194;Innvilget gammel §10-16 (før 1989);0;1;1;' as tekst from dual   
                  union select 25  as id, ';EGNE_BARN_GJR;194;Egne barn - Overgangsstønad;1;0;1;' as tekst from dual   
                  union select 26  as id, ';EGNE_BARN_KUN_GJR;194;Egne barn;0;1;1;' as tekst from dual   
                  union select 27  as id, ';UTDAN_GJR;194;Nødvendig utdanning - Overgangsstønad;1;0;1;' as tekst from dual   
                  union select 28  as id, ';OMS_AVD_BARN_GJR;194;Omsorg avdødes barn - Overgangsstønad;1;0;1;' as tekst from dual   
                  union select 29  as id, ';OMS_AVD_BARN_KUN_GJR;194;Omsorg avdødes barn;0;1;1;' as tekst from dual   
                  union select 30  as id, ';OMSTILL_GJR;194;Omstillingsperiode - Overgangsstønad;1;0;1;' as tekst from dual
 --                 union select   31  as id, '; ;;;;;;;;;;;;;;' from dual
                  )
            
              CONNECT BY instr(tekst, ';', 1, LEVEL) > 0
                         AND id = PRIOR id
                         AND PRIOR dbms_random.value IS NOT null
            )
          where id2 in (1,5)           
  group by id
),

------------------------------------------------------------------------------
-- CTE for å trekke ut de vedtakene som er løpende på dagens dato.
-- Vedtakstyper med tilhørende status er gjort som i eksisterende PC-mapping.
------------------------------------------------------------------------------
vedtak as (
   select vedtak_id,
          sak_id,
          kravhode_id,
          k_sak_t,
          person_id,
          dato_lopende_fom,
          dato_lopende_tom
     from pen.t_vedtak
    where k_sak_t = 'ALDER'  
      and k_vedtak_t in ('ENDRING', 'FORGANG', 'GOMR', 'SAMMENSTOT', 'OPPHOR', 'REGULERING')
      and k_vedtak_s in ('IVERKS', 'STOPPES', 'STOPPET', 'REAK')
      and dato_lopende_fom <= current_date
      and (dato_lopende_tom is null or dato_lopende_tom >= trunc(current_date))
),

---------------------------------------------------------------------------------------------------
-- CTE for å trekke ut de aktive vedtakene.
-- Denne kjører løpet via t_beregning_res og t_pen_under_utbet, ned mot ytelse_komp.
-- Dette er for de nyeste regelverkene forskjellig fra kap19 tom 2010.
-- Rader fra ytelse_komp traverseres og transponeres med aggregeringsfunksjoner opp på en rad per vedtak.
---------------------------------------------------------------------------------------------------
yk_bres as
(select v.vedtak_id,
        v.sak_id,
        v.kravhode_id,
        yk.pen_under_utbet_id as under_utbet_id,
        --Sum total
        max(puu.total_belop_brutto) as brutto_sum,
        max(puu.total_belop_netto) as netto_sum,
        --Grunnpensjon
        sum(case when k_ytelse_komp_t='GP' then yk.netto end) AS GP_NETTO,
        --Tilleggspensjon
        sum(case when k_ytelse_komp_t='TP' then yk.netto end) AS TP_NETTO,
        --Persontillegg
        sum(case when k_ytelse_komp_t='PT' then yk.netto end) AS PT_NETTO,
        --Særtillegg
        sum(case when k_ytelse_komp_t='ST' then yk.netto end) AS ST_NETTO,
        --Ektefelletillegg
        sum(case when k_ytelse_komp_t='ET' then netto else 0 end) AS ET_NETTO,
        --
        sum(case when k_ytelse_komp_t='MIN_NIVA_TILL_PPAR' then yk.netto end) AS MPN_SSTOT_NETTO,
        --
        sum(case when k_ytelse_komp_t='UT_ORDINER' then yk.netto end) AS UFOR_SUM_UT_ORD_NETTO,
        --Skjermingstillegg
        sum(case when k_ytelse_komp_t='SKJERMT' then yk.netto end) AS SKJERMT_NETTO,
        --Tillegg barn, særkull
        sum(case when yk.k_ytelse_komp_t='TSB' then yk.netto else 0 end) AS SAERKULL_NETTO,
        --Tillegg felles barn
        sum(case when yk.k_ytelse_komp_t='TFB' then yk.netto else 0 end) AS BARN_FELLES_NETTO,
        --Inntektspensjon
        sum(case when yk.k_ytelse_komp_t='IP' then yk.netto end) AS IP_NETTO,
        --Garantipensjon
        sum(case when yk.k_ytelse_komp_t='GAP' then yk.netto end) AS GAP_NETTO,
        --Gjenlevendetillegg
        sum(case when yk.k_ytelse_komp_t='AP_GJT' then yk.netto end) AS GJT_NETTO,
        --Gjenlevendetillegg kap19
        sum(case when yk.k_ytelse_komp_t='AP_GJT_KAP19' then yk.netto end) AS GJT_K19_NETTO,
        sum(case when yk.k_ytelse_komp_t='AP_GJT_KAP19' then yk.ap_kap19_med_gjr end) AS ap_kap19_med_gjr_bel,
        sum(case when yk.k_ytelse_komp_t='AP_GJT_KAP19' then yk.ap_kap19_uten_gjr end) AS ap_kap19_uten_gjr_bel,
        --
        sum(case when k_ytelse_komp_t='MIN_NIVA_TILL_INDV' then yk.netto end) AS MPN_INDIV_NETTO,
        max(k_minstepen_niva) as MINSTE_PEN_NIVA
   from vedtak v
        inner join pen.t_beregning_res br on br.vedtak_id = v.vedtak_id
                                         and br.dato_virk_fom <= current_date
                                         and (br.dato_virk_tom is null or br.dato_virk_tom >= trunc(current_date))
        inner join PEN.t_pen_under_utbet puu on puu.pen_under_utbet_id = br.pen_under_utbet_id 
        inner join pen.t_ytelse_komp yk on yk.pen_under_utbet_id = puu.pen_under_utbet_id
                                       and yk.bruk = '1'
                                       and yk.opphort = '0'
  group by v.vedtak_id, v.sak_id, v.kravhode_id, yk.pen_under_utbet_id
),

---------------------------------------------------------------------------------------
-- CTE for å trekke ut de aktive vedtakene.
-- Dette er for regelverket kap19 tom 2010.
-- Denne kjører løpet via t_beregning, ned mot ytelse_komp.
-- Rader fra ytelse_komp traverseres og transponeres med aggregeringsfunksjoner opp på en rad per vedtak.
---------------------------------------------------------------------------------------
yk_ber as
(select v.vedtak_id,
        v.sak_id,
        v.kravhode_id,
        yk.beregning_id as beregning_id,
        --Grunnpensjon
        sum(case when yk.k_ytelse_komp_t='GP' then yk.netto end) AS GP_NETTO,
        --Tilleggspensjon
        sum(case when yk.k_ytelse_komp_t='TP' then yk.netto end) AS TP_NETTO,
        --Persontillegg
        sum(case when k_ytelse_komp_t='PT' then yk.netto end) AS PT_NETTO,
        --Særtillegg
        sum(case when yk.k_ytelse_komp_t='ST' then yk.netto end) AS ST_NETTO,
        --Ektefelletillegg
        sum(case when yk.k_ytelse_komp_t='ET' then yk.netto else 0 end) AS ET_NETTO,
        --
        sum(case when k_ytelse_komp_t='MIN_NIVA_TILL_PPAR' then yk.netto end) AS MPN_SSTOT_NETTO,
        --
        sum(case when k_ytelse_komp_t='UT_ORDINER' then yk.netto end) AS UFOR_SUM_UT_ORD_NETTO,
        --Skjermingstillegg
        sum(case when yk.k_ytelse_komp_t='SKJERMT' then yk.netto end) AS SKJERMT_NETTO,
        --Tillegg barn, særkull
        sum(case when yk.k_ytelse_komp_t='TSB' then yk.netto else 0 end) AS SAERKULL_NETTO,
        --Tillegg felles barn
        sum(case when yk.k_ytelse_komp_t='TFB' then yk.netto else 0 end) AS BARN_FELLES_NETTO,
        --Inntektspensjon
        sum(case when yk.k_ytelse_komp_t='IP' then yk.netto end) AS IP_NETTO,
        --Garantipensjon
        sum(case when yk.k_ytelse_komp_t='GAP' then yk.netto end) AS GAP_NETTO,
        --Gjenlevendetillegg
        sum(case when yk.k_ytelse_komp_t='AP_GJT' then yk.netto end) AS GJT_NETTO,
        -- Gjenlevendetillegg kap19
        sum(case when yk.k_ytelse_komp_t='AP_GJT_KAP19' then yk.netto end) AS GJT_K19_NETTO,
        sum(case when yk.k_ytelse_komp_t='AP_GJT_KAP19' then yk.ap_kap19_med_gjr end) AS ap_kap19_med_gjr_bel,
        sum(case when yk.k_ytelse_komp_t='AP_GJT_KAP19' then yk.ap_kap19_uten_gjr end) AS ap_kap19_uten_gjr_bel,
        --
        sum(case when k_ytelse_komp_t='MIN_NIVA_TILL_INDV' then yk.netto end) AS MPN_INDIV_NETTO,
        max(k_minstepen_niva) as MINSTE_PEN_NIVA
   from vedtak v
        inner join pen.t_beregning b 
                on b.vedtak_id = v.vedtak_id
               and b.total_vinner = '1'
               and b.dato_virk_fom <= current_date
               and (b.dato_virk_tom is null or b.dato_virk_tom >= trunc(current_date)) 
        inner join pen.t_ytelse_komp yk 
                on yk.beregning_id = b.beregning_id
               and yk.bruk = '1'
               and yk.opphort = '0'                
  group by v.vedtak_id, v.sak_id, v.kravhode_id, yk.beregning_id
),

-------------------------------------------------------------------------------------------
-- CTE for å trekke ut de aktive vedtakene.
-- Kjører to disjunkte løp, joiner de to foregående CTE-ene, og tar med noen flere felter.
-- Tar med de dataelementene som enkelt kan joines inn.
-- De to løpene forenes med en UNION ALL, da de skal være disjunkte.
-------------------------------------------------------------------------------------------
aktive as
(select v.sak_id, 
        v.vedtak_id, 
        v.kravhode_id,
        vt.person_id,
        vt.k_sak_t,
        vt.k_vedtak_s,
        vt.k_vedtak_t,
        kh.k_regelverk_t as regelverk,
        kh.K_AFP_T,
        ug.uttaksgrad, 
        vt.dato_lopende_fom, 
        vt.dato_lopende_tom, 
        br.dato_virk_fom, 
        br.dato_virk_tom, 
        br.beregning_info_id AS beregning_info_id, 
        br_2011.beregning_info_id AS beregning_info_id_2016, 
        br_2025.beregning_info_id AS beregning_info_id_2025, 
        br.beregning_info_avdod AS beregning_info_id_avdod, 
        br_2011.beregning_info_avdod AS beregning_info_id_avdod_2016, 
        v.brutto_sum as brutto, --puu.total_belop_brutto as brutto, 
        v.netto_sum as netto, --puu.total_belop_netto as netto,
        v.GP_NETTO,
        v.TP_NETTO,
        v.PT_NETTO,
        v.ST_NETTO,
        v.ET_NETTO,
        v.SAERKULL_NETTO,
        v.BARN_FELLES_NETTO,
        v.MPN_SSTOT_NETTO,
        v.MPN_INDIV_NETTO,
        v.SKJERMT_NETTO,
        v.UFOR_SUM_UT_ORD_NETTO,
        v.GJT_NETTO,
        v.GJT_K19_NETTO,
        v.ap_kap19_med_gjr_bel,
        v.ap_kap19_uten_gjr_bel,
        v.IP_NETTO,
        v.GAP_NETTO,
        null as beregning_id,
        v.MINSTE_PEN_NIVA,
        null as minstepensjonist,
        null as yrksk_rett_flagg,
        null as yrksk_anv_flagg,
        null as red_pga_inst_opph_flagg,
        null as TT_ANVENDT_KAP19_ANTALL,
        0 as TT_ANVENDT_KAP20_ANTALL
   from yk_bres v
        inner join pen.t_vedtak vt on vt.vedtak_id = v.vedtak_id
        inner join pen.t_kravhode kh on kh.kravhode_id = v.kravhode_id
                                    and kh.sak_id = v.sak_id
        inner join pen.t_uttaksgrad ug on ug.kravhode_id = v.kravhode_id 
                                      and ug.dato_virk_fom <= current_date
                                      and (ug.dato_virk_tom is null or ug.dato_virk_tom >= trunc(current_date))
--                                      and ug.uttaksgrad <> 0
        inner join pen.t_beregning_res br on br.vedtak_id = v.vedtak_id
                                         and br.dato_virk_fom <= current_date
                                         and (br.dato_virk_tom is null or br.dato_virk_tom >= trunc(current_date))
        LEFT outer JOIN pen.t_BEREGNING_RES BR_2011 ON BR_2011.ber_res_ap_2011_2016_id = br.beregning_res_id 
        LEFT outer JOIN pen.t_BEREGNING_RES BR_2025 ON BR_2025.ber_res_ap_2025_2016_id = br.beregning_res_id 
 
 union all

 select v.sak_id, 
        v.vedtak_id, 
        v.kravhode_id,
        vt.person_id,
        vt.k_sak_t,
        vt.k_vedtak_s,
        vt.k_vedtak_t,
        nvl(kh.k_regelverk_t, 'G_REG') as regelverk, 
        kh.K_AFP_T,
--        100 as uttaksgrad, 
        case
          when nvl(b.netto,0) = 0 then 0
          when nvl(b.brutto,0) = 0 then 100
          else ROUND(b.netto / b.brutto) * 100
        end as uttaksgrad,
        vt.dato_lopende_fom, 
        vt.dato_lopende_tom, 
        b.dato_virk_fom, 
        b.dato_virk_tom, 
        null AS beregning_info_id, 
        null AS beregning_info_id_2016, 
        null as beregning_info_id_2025,
        null AS beregning_info_id_avdod, 
        null AS beregning_info_id_avdod_2016, 
        b.brutto, 
        b.netto,
        v.GP_NETTO,
        v.TP_NETTO,
        v.PT_NETTO,
        v.ST_NETTO,
        v.ET_NETTO,
        v.SAERKULL_NETTO,
        v.BARN_FELLES_NETTO,
        v.MPN_SSTOT_NETTO,
        v.MPN_INDIV_NETTO,
        v.SKJERMT_NETTO,
        v.UFOR_SUM_UT_ORD_NETTO,
        v.GJT_NETTO,
        v.GJT_K19_NETTO,
        v.ap_kap19_med_gjr_bel,
        v.ap_kap19_uten_gjr_bel,
        v.IP_NETTO,
        v.GAP_NETTO,
        b.beregning_id,
        v.MINSTE_PEN_NIVA,
        case 
            when b.k_minstepensj_t = 'IKKE_MINST_PEN' then '0'
            when b.k_minstepensj_t = 'ER_MINST_PEN' then '1'
            else null
        end as minstepensjonist,
        case
            when nvl(b.yug,0) > 0 then 1
            when b.k_resultat_t in ('UP_GJP_UP_YP', 'GJP_UP_YP', 'AP_GJP_UP_YP', 'UP_YP', 'AP2011_GJP_UP_YP')
               then 1
            else 0
        end as yrksk_rett_flagg,
        case
            when nvl(b.yug,0) > 0 then 1
            else 0
        end as yrksk_anv_flagg,
        b.red_pga_inst_opph as red_pga_inst_opph_flagg,
        b.tt_anv as TT_ANVENDT_KAP19_ANTALL,
        null as TT_ANVENDT_KAP20_ANTALL
   from yk_ber v
        inner join pen.t_vedtak vt on vt.vedtak_id = v.vedtak_id
        inner join pen.t_kravhode kh on kh.kravhode_id = v.kravhode_id
        inner join pen.t_beregning b on b.vedtak_id = v.vedtak_id
                                    and b.total_vinner = '1'
                                    and b.dato_virk_fom <= current_date
                                    and (b.dato_virk_tom is null or b.dato_virk_tom >= trunc(current_date))    
),

--------------------------------------------------
-- En CTE som brukes for overgangsstønad.  
--------------------------------------------------
tvvx as
(select x.vedtak_id, 
        rtrim(x.k_vilk_vurd_t) as k_vilk_vurd_t
   from (select aktive.vedtak_id, 
                tvv.k_vilk_vurd_t,
                row_number() over(partition by aktive.vedtak_id order by aktive.vedtak_id) as rn
           from aktive 
                left outer join pen.t_vilkar_vedtak tvv on tvv.vedtak_id = aktive.vedtak_id
          where tvv.dato_virk_fom <= current_date
            and (tvv.dato_virk_tom >= trunc(current_date) or tvv.dato_virk_tom is null)
         ) x
   where x.rn = 1
)

------------------------------------------------------------------------------
-- Selve uttrekket av data skjer her.
-- Rader fra CTE-ene ovenfor, men henter også inn noen flere opplysninger.
------------------------------------------------------------------------------
SELECT to_number(to_char(current_date,'YYYYMMDDHH24MI')) as periode,
       --to_number(to_char(current_date,'YYYYMMDDHH24MI')) as periode,
       aktive.vedtak_id,
       aktive.sak_id,
       aktive.kravhode_id,
       aktive.k_vedtak_t,
       aktive.k_vedtak_s,
--       person.fnr_fk as persnr,
       null as persnr,
       aktive.person_id,
       aktive.dato_lopende_fom,
       aktive.dato_lopende_tom,
       
       --AFP-ordning
       case aktive.K_AFP_T
           when 'LONHO' then 'LO/NHO - ordningen'
           when 'FINANS' then 'Finansnæringen'
           when 'NAVO' then 'Spekter'
           when 'AFPKOM' then 'AFP - Kommunalsektor'
           when 'AFPSTAT' then 'AFP - Stat'
           when 'KONV_K' then 'Konvertert privat'
           when 'KONV_O' then 'Konvertert offentlig'
       end as AFP_ordning,
       
--       aktive.regelverk as regelverk_kode,
       case aktive.regelverk
           when 'N_REG_G_OPPTJ' then 'AP kap 19 fom 2011'
           when 'N_REG_G_N_OPPTJ' then 'AP kap. 19 og kap. 20'
           when 'G_REG' then 'AP kap 19 tom 2010'
           when 'N_REG_N_OPPTJ' then 'AP kap. 20'
           else 'Ukjent'
       end as regelverk,
       --
       aktive.k_sak_t as Sakstype,
       case when aktive.uttaksgrad <= 100 then aktive.uttaksgrad
            else null
       end as uttaksgrad,
       aktive.netto,
       case
          when aktive.k_sak_t = 'ALDER' 
               and nvl(aktive.netto,0) > 0
             then 1
          else 0
       end as AldersytelseFlagg,
       to_number(coalesce(aktive.minstepensjonist, bi.mottar_min_pensjonsniva)) as minstepensjon,
       aktive.MINSTE_PEN_NIVA,
       case 
          when ost.flagg = 1 and nvl(aktive.netto,0) > 0
              then 1
          else 0
       end as Overgangsstonad,
       aktive.GP_NETTO,
       aktive.TP_NETTO,
       aktive.PT_NETTO,
       aktive.ST_NETTO,
       aktive.ET_NETTO,
       aktive.SAERKULL_NETTO,
       aktive.BARN_FELLES_NETTO,
       aktive.MPN_SSTOT_NETTO,
       aktive.MPN_INDIV_NETTO,
       aktive.SKJERMT_NETTO,
       aktive.UFOR_SUM_UT_ORD_NETTO,
       aktive.GJT_NETTO,
       aktive.GJT_K19_NETTO,
       aktive.ap_kap19_med_gjr_bel,
       aktive.ap_kap19_uten_gjr_bel,
       aktive.IP_NETTO,
       aktive.GAP_NETTO,
       
       -- Yrkesskaderett --
       coalesce(aktive.yrksk_rett_flagg, 
                case
                   when nvl(bi.yrksk_reg,'0') = '1' then 1
                   when nvl(bi.rett_pa_gjlevenderett,'0') = '1' and nvl(bi_avdod.yrksk_reg,'0') = '1' then 1
                   else 0
                end) as yrkesskade_rett_flagg,
       
       -- Anvendt yrkesskaderett.
       coalesce(aktive.yrksk_anv_flagg, 
                case
                   when nvl(bi.yrksk_anv,'0') = '1' then 1
                   when nvl(bi.gjenlevrett_anv,'0') = '1' and nvl(bi_avdod.yrksk_anv,'0') = '1' then 1
                   else 0
                end) as yrkesskade_anv_flagg,
       
       -- Redusert pga. institusjonsopphold.
       to_number(coalesce(aktive.red_pga_inst_opph_flagg, bi.INST_OPPH_ANV)) as red_pga_inst_opph_flagg,
       
       -- Anvendt trygdetid.
       coalesce(aktive.TT_ANVENDT_KAP19_ANTALL, bi.TT_ANV) as TT_ANVENDT_KAP19_ANTALL,
       coalesce(case 
                    when aktive.regelverk = 'N_REG_G_N_OPPTJ' then bi_2025.TT_ANV 
                end, 
                aktive.TT_ANVENDT_KAP20_ANTALL
               ) as TT_ANVENDT_KAP20_ANTALL,

       -- Sjekker om det eksisterer annen sak/vedtak om AFP_PRIVAT for denne personen.
       case
          when exists (select null
                         from pen.t_vedtak v2
                        where v2.person_id = aktive.person_id
                          and v2.dato_lopende_fom <= trunc(current_date)
                          and (v2.dato_lopende_tom is null or v2.dato_lopende_tom >= trunc(current_date))
                          and v2.k_sak_t = 'AFP_PRIVAT'
                          and not exists (select null
                                            from pen.t_UTTAKSGRAD u2 
                                           where u2.sak_id = v2.sak_id 
                                             and u2.dato_virk_fom <= trunc(current_date) 
                                             and (u2.DATO_VIRK_TOM is null or u2.DATO_VIRK_TOM >= trunc(current_date)) 
                                             and u2.uttaksgrad = 0)
                       ) then 1
          else 0
       end as AFP_PRIVAT_FLAGG,
       
       -- Sjekker om det eksisterer innvilget gjenlevenderett.
       case
          when exists (select null
                         FROM PEN.T_VILKAR_VEDTAK PVV 
                        WHERE pvv.vedtak_id = aktive.vedtak_id
                          and pvv.K_KRAVLINJE_T = 'GJR' 
                          AND pvv.K_VILKAR_RESUL_T = 'INNV'
                          AND PVV.dato_virk_fom < current_date
                          AND (PVV.dato_virk_tom >= trunc(current_date) 
                               OR PVV.dato_virk_tom IS NULL)
                      ) then 1
          else 0
       end as innv_gj_rett,
       
       -- Sjekker om det er afp kommunal ytelse på afp_historikk.
       case
           when exists (select null
                          FROM PEN.t_AFP_HISTORIK a
                               join pen.t_person p on p.person_id = a.person_id
                         WHERE aktive.person_id = p.person_id
                           and a.k_afp_t = 'AFPKOM' 
                           AND a.bruk = 1
                           AND a.afp_pensjonsgrad > 0
                           --AFP kommunal i alderen 62 år og en måned til 67 år og en måned.
                           AND MONTHS_BETWEEN(current_date, p.dato_fodsel) BETWEEN (62*12+1) AND (67*12+1)
                           AND a.virk_fom <= current_date
                           and (a.virk_tom >= trunc(current_date) or a.virk_tom is null)
                        ) then 1
                 else 0
       end as kommunal_ytelse,           
       --
       to_timestamp(to_char(current_timestamp, 'DD.MM.YYYY HH24.MI.SS,FF9')) as kjoretidspunkt
  FROM aktive 
      
       --- Beregningsinfo ---------------------------
       left outer join pen.t_beregning_info bi 
               on bi.beregning_info_id = case 
                                            when aktive.regelverk = 'N_REG_G_N_OPPTJ' then aktive.beregning_info_id_2016 
                                            else aktive.beregning_info_id 
                                         end 
       left outer join pen.t_beregning_info bi_avdod 
                on bi_avdod.beregning_info_id = case 
                                                   when aktive.regelverk = 'N_REG_G_N_OPPTJ' then aktive.beregning_info_id_avdod_2016 
                                                   else aktive.beregning_info_id_avdod 
                                                end 
--               and bi_avdod.dato_virk_fom <= trunc(current_date)
--               and (bi_avdod.dato_virk_tom is null or bi_avdod.dato_virk_tom >= trunc(current_date))
       left outer join pen.t_beregning_info bi_2025 on bi_2025.beregning_info_id = aktive.beregning_info_id_2025

       ---- Overgangsstønad ---------------------------------
       left outer join tvvx on tvvx.vedtak_id = aktive.vedtak_id 
       left outer join overgangsstonad ost on ost.kode = tvvx.k_vilk_vurd_t
       ------------------------------------------------------
