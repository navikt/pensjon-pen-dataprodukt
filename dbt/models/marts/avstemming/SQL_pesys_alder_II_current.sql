
--create or replace view vw_pesys_alder_ii_current as
-------------------------------------------------------------------------------
-- Eirik Grønli
-- 28. aug 2025
--
-- Endret: 
-------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- Starter først med definisjoner av høvelige CTEs (Common Table Expressions).
------------------------------------------------------------------------------
with 

-------------------------------------------------------------------------------
-- CTE for å trekke ut de vedtakene som er løpende på siste dato forrige måned.
-- Vedtakstyper med tilhørende status er gjort som i eksisterende PC-mapping.
-------------------------------------------------------------------------------
vedtak as (
   select v.vedtak_id,
          v.sak_id,
          v.kravhode_id,
          v.k_sak_t,
          v.person_id,
          v.dato_lopende_fom,
          v.dato_lopende_tom
     from pen.t_vedtak v
    where k_sak_t = 'ALDER'  
      and k_vedtak_t in ('ENDRING', 'FORGANG', 'GOMR', 'SAMMENSTOT', 'OPPHOR', 'REGULERING')
      and k_vedtak_s in ('IVERKS', 'STOPPES', 'STOPPET', 'REAK')
      and dato_lopende_fom <= current_date
      and (dato_lopende_tom is null or dato_lopende_tom >= trunc(current_date))
),

---------------------------------------------------------------------------------------------------
-- CTE
-- Denne kjører løpet via t_beregning_res og t_pen_under_utbet, ned mot ytelse_komp.
-- Dette er for de nyeste regelverkene forskjellig fra kap19 tom 2010.
-- Rader fra ytelse_komp traverseres og transponeres med aggregeringsfunksjoner opp på en rad per vedtak.
---------------------------------------------------------------------------------------------------
yk_bres as
(select v.vedtak_id,
        v.sak_id,
        v.kravhode_id,
        puu.pen_under_utbet_id as under_utbet_id,
        max(puu.total_belop_brutto) as brutto,
        max(puu.total_belop_netto) as netto,
        sum(case when yk.k_ytelse_komp_t='VT' then yk.netto end) AS VT_NETTO,
        sum(case when yk.k_ytelse_komp_t='PT' then yk.netto end) AS PT_NETTO,
        sum(case when yk.k_ytelse_komp_t='TP' then yk.netto end) AS TP_NETTO,
        max(case when yk.k_ytelse_komp_t='GP' then yk.PSATS_GP end) as GP_SATS_BELOP,
        sum(case when yk.k_ytelse_komp_t='AFP_T' then yk.netto else 0 end) AS AFP_T_NETTO,
        sum(case when yk.k_ytelse_komp_t='AFP_LIVSVARIG' then yk.netto else 0 end) AS AFP_LIVSVARIG_NETTO
   from vedtak v 
        inner join pen.t_beregning_res br on br.vedtak_id = v.vedtak_id
                                         and br.dato_virk_fom <= current_date
                                         and (br.dato_virk_tom is null or br.dato_virk_tom >= trunc(current_date))
        inner join PEN.t_pen_under_utbet puu on puu.pen_under_utbet_id = br.pen_under_utbet_id 
        inner join pen.t_ytelse_komp yk on yk.pen_under_utbet_id = puu.pen_under_utbet_id
                                       and yk.bruk = '1'
                                       and yk.opphort = '0'      
  group by v.vedtak_id, v.sak_id, v.kravhode_id, puu.pen_under_utbet_id
),

--------------------------------------------------------------------
-- CTE
-- For å hente pro rata teller og nevner.
-- Går til t_ytelse_komp med type=PT, forbi t_pen_under_utbet.
--------------------------------------------------------------------
yk_prorata as (
  select v.vedtak_id,
         mpn2.pro_rata_teller_mnd, 
         mpn2.pro_rata_nevner_mnd
    from vedtak v 
         inner join pen.t_kravhode k on k.kravhode_id = v.kravhode_id
                                    and nvl(k.k_regelverk_t, 'G_REG') <> 'G_REG'
         inner join pen.t_beregning_res br on br.vedtak_id = v.vedtak_id
                                          and br.dato_virk_fom <= current_date
                                          and (br.dato_virk_tom is null or br.dato_virk_tom >= trunc(current_date))
         left outer join pen.t_BEREGNING_RES BRES_2011 ON BRES_2011.ber_res_ap_2011_2016_id = br.beregning_res_id 
         left outer join pen.t_beregning_2011 ber_2011 on ber_2011.BEREGNING_RES_ID = 
                                                                              case
                                                                                  when k.k_regelverk_t = 'N_REG_G_N_OPPTJ'
                                                                                        then bres_2011.beregning_res_id 
                                                                                  else br.beregning_res_id 
                                                                              end  
         inner join pen.t_basispensjon bp on bp.basispensjon_id = ber_2011.basispensjon
         inner join pen.t_ytelse_komp yk2 on yk2.ytelse_komp_id = bp.pensjonstillegg
                                         and yk2.bruk = '1'
                                         and yk2.opphort = '0'
                                         and yk2.k_ytelse_komp_t = 'PT'
         inner join pen.t_f_min_pen_niva fmpn on fmpn.F_MIN_PEN_NIVA_ID = yk2.F_MIN_PEN_NIVA_ID
         inner join pen.t_min_pen_niva mpn2 on mpn2.MIN_PEN_NIVA_ID = fmpn.MIN_PEN_NIVA_ID
                                           and mpn2.pro_rata_nevner_mnd > 0
   where nvl(k.k_regelverk_t, 'G_REG') <> 'G_REG'
),

---------------------------------------------------------------------------------------
-- CTE
-- Dette er for regelverket kap19 tom 2010.
-- Denne kjører løpet via t_beregning, ned mot ytelse_komp.
-- Rader fra ytelse_komp traverseres og transponeres med aggregeringsfunksjoner opp på en rad per vedtak.
---------------------------------------------------------------------------------------
yk_ber as
(select v.vedtak_id,
        v.sak_id,
        v.kravhode_id,
        yk.beregning_id as beregning_id,
        sum(case when yk.k_ytelse_komp_t='VT' then yk.netto end) AS VT_NETTO,
        sum(case when yk.k_ytelse_komp_t='PT' then yk.netto end) AS PT_NETTO,
        sum(case when yk.k_ytelse_komp_t='TP' then yk.netto end) AS TP_NETTO,
        max(case when b.k_minstepensj_t = 'ER_MINST_PEN' and yk.k_ytelse_komp_t = 'MIN_NIVA_TILL_INDV' then mpn2.Sats end) as mpn_sats,
        max(case when yk.k_ytelse_komp_t='GP' then yk.PSATS_GP end) as GP_SATS_BELOP,
        sum(case when yk.k_ytelse_komp_t='AFP_T' then yk.netto else 0 end) AS AFP_T_NETTO,
        sum(case when yk.k_ytelse_komp_t='AFP_LIVSVARIG' then yk.netto else 0 end) AS AFP_LIVSVARIG_NETTO,
        max(case when yk.k_ytelse_komp_t='GP' then brok.teller end) as prorata_teller,  
        max(case when yk.k_ytelse_komp_t='GP' then brok.nevner end) as prorata_nevner 
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
        left outer join pen.t_anvendt_trygdetid tat on tat.anvendt_trygdetid_id = yk.anvendt_trygdetid
        left outer join pen.t_brok brok on brok.brok_id = tat.pro_rata
        left outer join pen.t_min_pen_niva mpn2 on mpn2.min_pen_niva_id = yk.min_pen_niva_id
  group by v.vedtak_id, v.sak_id, v.kravhode_id, yk.beregning_id
),

-----------------------------------------------------------------
-- CTE for å transponere de tre forskjellige beholdningsbeløpene.
-- Dette gjelder bare 2016-greiene, dvs det ene regelverket.
-----------------------------------------------------------------
beholdning as (
   select v.vedtak_id, 
          sum(case when bh.k_beholdning_t = 'PEN_B' then bh.totalbelop end) as BEHOLDNING_PENSJON_BELOP,
          sum(case when bh.k_beholdning_t = 'GAR_PEN_B' then bh.totalbelop end) as BEHOLDNING_GARAN_PEN_BELOP,
          sum(case when bh.k_beholdning_t = 'GAR_T_B' then bh.totalbelop end) as BEHOLDNING_GARAN_TLG_BELOP
     from pen.t_vedtak  v
          inner join pen.t_kravhode kh on kh.kravhode_id = v.kravhode_id 
                                      and kh.k_regelverk_t = 'N_REG_G_N_OPPTJ'
          inner join pen.t_beregning_res br 
                      on br.vedtak_id = v.vedtak_id
                     and br.dato_virk_fom <= current_date
                     and (br.dato_virk_tom is null or br.dato_virk_tom >= trunc(current_date))
          inner join pen.T_BEREGNING_RES BR_2025 ON BR_2025.ber_res_ap_2025_2016_id = br.beregning_res_id
          inner join pen.t_beregning_2011 br_2011 on br_2011.beregning_res_kap_20 = br_2025.beregning_res_id
          inner join pen.t_beholdninger bhr on bhr.beholdninger_id = br_2011.beholdninger_id
          inner join pen.t_beholdning bh 
                  on bh.beholdninger_id = bhr.beholdninger_id
                 and bh.dato_fom <= current_date
                 and (bh.dato_tom is null or bh.dato_tom >= trunc(current_date)) 
    where bh.beholdning_id is not null
      and v.k_sak_t = 'ALDER'  
      and v.k_vedtak_t in ('ENDRING', 'FORGANG', 'GOMR', 'SAMMENSTOT', 'OPPHOR', 'REGULERING')
      and v.k_vedtak_s in ('IVERKS', 'STOPPES', 'STOPPET', 'REAK')
      and v.dato_lopende_fom <= current_date
      and (v.dato_lopende_tom is null or v.dato_lopende_tom >= trunc(current_date))
    group by v.vedtak_id
),

---------------------------------------------------------------------------------------
-- CTE for å hente pensjonsgivende inntekt.
-- Denne kjører løpet via t_kravhode + t_person mot t_persongrunnlag og t_inntekt.
---------------------------------------------------------------------------------------
inntekt as(
    select  inntekter.person_id as person_id,
            inntekter.kravhode_id as kravhode_id,
            max(case when inntekter.k_inntekt_t='SFAKPI' then inntekter.INNTEKT_BELOP else 0 end) as INNTEKT_SFAKPI_BELOP,
            max(case when inntekter.k_inntekt_t='PENSKD' then inntekter.INNTEKT_BELOP else 0 end) as INNTEKT_PENSKD_BELOP,
            max(case when inntekter.k_inntekt_t='FKI' then inntekter.INNTEKT_BELOP else 0 end) as INNTEKT_FKI_BELOP,
            max(case when inntekter.k_inntekt_t='FPI' then inntekter.INNTEKT_BELOP else 0 end) as INNTEKT_FPI_BELOP,
            max(case when inntekter.k_inntekt_t='PENT' then inntekter.INNTEKT_BELOP else 0 end) as INNTEKT_PENT_BELOP
      from (select distinct person_id, kravhode_id, k_inntekt_t,
                   LAST_VALUE(belop) OVER (PARTITION BY person_id, kravhode_id, k_inntekt_t 
                                           ORDER BY dato_fom ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as INNTEKT_BELOP
            from (select pg.person_id, 
                         pg.kravhode_id, 
                         i.belop, 
                         i.dato_fom, 
                         i.k_inntekt_t --, it.k_inntekt_t, i.k_grunnlag_kilde 
                    from pen.t_vedtak v
                         inner join pen.t_person_grunnlag pg on pg.kravhode_id = v.kravhode_id
                         inner join pen.t_inntekt i
                              on pg.person_grunnlag_id = i.person_grunnlag_id
                             and i.bruk = 1
                   where pg.person_id is not null
                     and pg.kravhode_id is not null 
                     and i.bruk = 1 
                     and v.k_sak_t = 'ALDER'  
                     and v.k_vedtak_t in ('ENDRING', 'FORGANG', 'GOMR', 'SAMMENSTOT', 'OPPHOR', 'REGULERING')
                     and v.k_vedtak_s in ('IVERKS', 'STOPPES', 'STOPPET', 'REAK')
                     and v.dato_lopende_fom <= current_date
                     and (v.dato_lopende_tom is null or v.dato_lopende_tom >= trunc(current_date))
                     and to_char(i.dato_fom, 'YYYYMM') <= to_char(to_date(current_date), 'YYYYMM') 
                     and (i.dato_tom is null 
                           or to_char(i.dato_tom, 'YYYYMM') >= to_char(current_date, 'YYYYMM'))
                 )
           ) inntekter
    group by inntekter.person_id, inntekter.kravhode_id
),

-------------------------------------------------------------------------
-- CTE for inntektskomponenter for ektefelle/samboer/partner.
------------------------------------------------------------------------- 
inntekt_eps as(
    select  vedtak_id,
            max(case when k_inntekt_t='SFAKPI' then INNTEKT_BELOP else 0 end) as INNTEKT_SFAKPI_BELOP,
            max(case when k_inntekt_t='PENSKD' then INNTEKT_BELOP else 0 end) as INNTEKT_PENSKD_BELOP,
            max(case when k_inntekt_t='FKI' then INNTEKT_BELOP else 0 end) as INNTEKT_FKI_BELOP,
            max(case when k_inntekt_t='FPI' then INNTEKT_BELOP else 0 end) as INNTEKT_FPI_BELOP,
            max(case when k_inntekt_t='PENT' then INNTEKT_BELOP else 0 end) as INNTEKT_PENT_BELOP
      from (select distinct person_id, kravhode_id, vedtak_id, k_inntekt_t,
                   LAST_VALUE(belop) OVER (PARTITION BY person_id, kravhode_id, vedtak_id, k_inntekt_t 
                                           ORDER BY dato_fom ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as INNTEKT_BELOP
            from (select pg.person_id, 
                         pg.kravhode_id, 
                         v.vedtak_id,
                         i.belop, 
                         i.dato_fom, i.dato_tom,
                         i.k_inntekt_t 
                    from pen.t_vedtak v
                         inner join pen.t_person_grunnlag pg on pg.kravhode_id = v.kravhode_id
                         inner join pen.t_person_det pd
                                on pd.person_grunnlag_id = pg.person_grunnlag_id
                               and pd.dato_fom <= current_date
                               and (pd.dato_tom is null or pd.dato_tom >= current_date)
                               and pd.bruk = 1
                         inner join pen.t_inntekt i
                              on pg.person_grunnlag_id = i.person_grunnlag_id
                   where pg.person_id  is not null
                     and pg.kravhode_id is not null 
                     and i.bruk = 1 
                     and pd.k_bor_med_t in ('J_EKTEF', 'N_GIFT', 'SAMBOER1_5', 'SAMBOER3_2', 'J_PARTNER', 'N_GIFT_P') 
                     and v.k_sak_t = 'ALDER'  
                     and v.k_vedtak_t in ('ENDRING', 'FORGANG', 'GOMR', 'SAMMENSTOT', 'OPPHOR', 'REGULERING')
                     and v.k_vedtak_s in ('IVERKS', 'STOPPES', 'STOPPET', 'REAK')
                     and v.dato_lopende_fom <= current_date
                     and (v.dato_lopende_tom is null or v.dato_lopende_tom >= trunc(current_date))
                     and to_char(i.dato_fom, 'YYYYMM') <= to_char(to_date(current_date), 'YYYYMM') 
                     and (i.dato_tom is null 
                          or to_char(i.dato_tom, 'YYYYMM') >= to_char(current_date, 'YYYYMM'))
                 )
           ) inntekt
    group by vedtak_id 
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
        br_2025.beregning_res_id AS beregning_res_id_2025, 
        v.brutto,  --puu.total_belop_brutto as brutto, 
        v.netto,   --puu.total_belop_netto as netto,
        v.vt_netto,
        v.pt_netto,
        v.AFP_T_NETTO,
        v.AFP_LIVSVARIG_NETTO,
        null as pen_under_utbet_id, --puu.pen_under_utbet_id,
        null as beregning_id,
        null as minstepensjonist,
        null as beregning_kode,
        case
           when kh.k_regelverk_t = 'N_REG_G_N_OPPTJ'
               then br_2011.k_bor_med_t
           else br.k_bor_med_t
        end as k_bor_med_t,
        null as mpn_sats,
        null as mpn_aarsak,
        v.GP_SATS_BELOP,
        pro.pro_rata_teller_mnd as prorata_teller,
        pro.pro_rata_nevner_mnd as prorata_nevner,
        beh.BEHOLDNING_PENSJON_BELOP,
        beh.BEHOLDNING_GARAN_PEN_BELOP,
        beh.BEHOLDNING_GARAN_TLG_BELOP
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
        left outer join pen.t_BEREGNING_RES BR_2011 ON BR_2011.ber_res_ap_2011_2016_id = br.beregning_res_id 
        left outer join pen.T_BEREGNING_RES BR_2025 ON BR_2025.ber_res_ap_2025_2016_id = br.beregning_res_id 
        left outer join beholdning beh on beh.vedtak_id = v.vedtak_id
        left outer join yk_prorata pro on pro.vedtak_id = v.vedtak_id

 union all

 select v.sak_id, 
        v.vedtak_id, 
        v.kravhode_id,
        vt.person_id,
        vt.k_sak_t,
        nvl(kh.k_regelverk_t, 'G_REG') as regelverk, 
        kh.K_AFP_T,
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
        null as beregning_res_id_2025,
        b.brutto, 
        b.netto,
        v.vt_netto,
        v.pt_netto,
        v.AFP_T_NETTO,
        v.AFP_LIVSVARIG_NETTO,
        null as pen_under_utbet_id,
        b.beregning_id,
        case 
            when b.k_minstepensj_t = 'IKKE_MINST_PEN' then '0'
            when b.k_minstepensj_t = 'ER_MINST_PEN' then '1'
            else null
        end as minstepensjonist,
        b.K_BEREG_METODE_T as beregning_kode,
        b.k_bor_med_t,
        v.mpn_sats,
        b.K_MINSTEPENSJ_ARSAK mpn_aarsak,
        v.GP_SATS_BELOP,
        v.prorata_teller,
        v.prorata_nevner,
        null as BEHOLDNING_PENSJON_BELOP,
        null as BEHOLDNING_GARAN_PEN_BELOP,
        null as BEHOLDNING_GARAN_TLG_BELOP
   from yk_ber v
        inner join pen.t_vedtak vt on vt.vedtak_id = v.vedtak_id
        inner join pen.t_kravhode kh on kh.kravhode_id = v.kravhode_id
        inner join pen.t_beregning b on b.vedtak_id = v.vedtak_id
                                    and b.total_vinner = '1'
                                    and b.dato_virk_fom <= current_date
                                    and (b.dato_virk_tom is null or b.dato_virk_tom >= trunc(current_date))    
)

--,endelig as (
------------------------------------------------------------------------------
-- Selve uttrekket av data skjer her.
-- Rader fra CTE-ene ovenfor, men henter også inn noen flere opplysninger.
------------------------------------------------------------------------------
SELECT --distinct
       to_number(to_char(current_date,'YYYYMMDDHH24MI')) as periode,
       aktive.vedtak_id,
       aktive.sak_id,
       aktive.kravhode_id,
--       person.fnr_fk as persnr,
       null as persnr,
       aktive.person_id,
       aktive.dato_lopende_fom,
       aktive.dato_lopende_tom,
       case aktive.regelverk
           when 'G_REG' then 'AP kap 19 tom 2010'
           when 'N_REG_G_OPPTJ' then 'AP kap 19 fom 2011'
           when 'N_REG_G_N_OPPTJ' then 'AP kap. 19 og kap. 20'
           when 'N_REG_N_OPPTJ' then 'AP kap. 20'
           else 'Ukjent'
       end as regelverk,
       aktive.k_sak_t as Sakstype,
       case 
          when aktive.uttaksgrad <= 100 then aktive.uttaksgrad
          else null
       end as uttaksgrad,
       case
          when aktive.k_sak_t = 'ALDER' 
               and nvl(aktive.netto,0) > 0
             then 1
          else 0
       end as AldersytelseFlagg,
       to_number(
           case
              when aktive.regelverk = 'N_REG_G_N_OPPTJ' then bi_2016.mottar_min_pensjonsniva
              when aktive.regelverk <> 'G_REG' then bi.mottar_min_pensjonsniva
              when aktive.regelverk = 'G_REG' then aktive.minstepensjonist
              else null
           end 
       ) as minstepensjon,
       aktive.brutto,
       aktive.netto,
       aktive.vt_netto,
       aktive.pt_netto,
       aktive.AFP_T_NETTO,
       aktive.AFP_LIVSVARIG_NETTO,
       
       case
          when aktive.regelverk = 'G_REG' then aktive.beregning_kode
          when aktive.regelverk = 'N_REG_G_N_OPPTJ' then bi_2016.K_BEREG_METODE_T
          when aktive.regelverk != 'G_REG' then bi.K_BEREG_METODE_T
       end as beregning_kode,
       
       aktive.k_bor_med_t,
       case 
          when aktive.k_bor_med_t in ('GLAD_EKT', 'J_EKTEF', 'N_GIFT')
                 then 'EKTEF'
          when aktive.k_bor_med_t in ('SAMBOER1_5', 'SAMBOER3_2', 'N_SAMBOER')
                 then 'SAMBO'
          when aktive.k_bor_med_t in ('J_PARTNER', 'GLAD_PART', 'N_GIFT_P')
                 then 'PARTNER'
          when aktive.k_bor_med_t in ('N_SOSKEN', 'J_SOSKEN')
                 then 'SOSKEN'
          when aktive.k_bor_med_t in ('J_BARN', 'N_BARN')
                 then 'BARN'
          when aktive.k_bor_med_t in ('J_FBARN', 'N_FBARN')
                 then 'FBARN'
          else null
       end as k_grnl_rolle_t,

       case 
          when aktive.regelverk <> 'G_REG' and bi.mottar_min_pensjonsniva = 1 then mpn3.Sats
          when aktive.regelverk = 'G_REG' then aktive.mpn_sats 
          else null
       end as mpn_arsak_sats,
       
       case
          when aktive.regelverk = 'N_REG_G_N_OPPTJ' then bi_2025.MOTTAR_MIN_PENSJNIVA_ARSAK
          when aktive.regelverk = 'G_REG' then aktive.mpn_aarsak
          when aktive.regelverk != 'G_REG' then  bi.MOTTAR_MIN_PENSJNIVA_ARSAK
          else null
       end as mpn_aarsak_kode,
       
       ------------------------------------------------------
       -- mpn_aarsak_flagg
       case 
           -- 2016-greier
           when aktive.regelverk = 'N_REG_G_N_OPPTJ'
              then case
                      when bi_2025.MOTTAR_MIN_PENSJNIVA_ARSAK is null
                         then 0 
                      else case
                              when bi_2025.MOTTAR_MIN_PENSJNIVA_ARSAK not like '%POS_YTELSE_UNDER_MIN%' 
                                 and bi_2025.MOTTAR_MIN_PENSJNIVA_ARSAK like '%POS_MNT_PENSJONISTPAR%' 
                                   then 0
                              else nvl(to_number(bi_2025.MOTTAR_MIN_PENSJONSNIVA),0)
                           end
                   end
                   
           -- ikke-2016-greier og ikke 1967-greier
           when aktive.regelverk != 'G_REG' 
              then case
                     when bi.MOTTAR_MIN_PENSJNIVA_ARSAK is null
                         then 0
                     else case 
                             when bi.MOTTAR_MIN_PENSJNIVA_ARSAK not like '%POS_MOTTAR_MINSTENIVA%' 
                               and bi.MOTTAR_MIN_PENSJNIVA_ARSAK like '%POS_MNT_PENSJONISTPAR%'
                                  then 0 
                             else to_number(bi.MOTTAR_MIN_PENSJONSNIVA)
                     end
                   end
                   
           --1967-greier
           when aktive.regelverk = 'G_REG' 
             then 
                case
                   when aktive.mpn_aarsak is null
                      or (  aktive.mpn_aarsak like '%POS_MNT_PENSJONISTPAR%'
                        and aktive.mpn_aarsak not like '%POS_BRUTTO_ST_OVER_NULL%'
                        and aktive.mpn_aarsak not like '%POS_YTELSE_UNDER_MIN%'
                        )
                         then 0
                   else case 
                           when aktive.minstepensjonist = '1' then 1 
                           else 0 
                        end
                end
           
           -- skal aldri komme hit, men for sikkerhets skyld
           else 0
       end as mpn_aarsak_flagg,
       ------------------------------------------------
       
       case when nvl(aktive.regelverk,'G_REG') != 'G_REG' then 1 else 0 end as nytt_regelverk_flagg,
       
       case
          when aktive.GP_SATS_BELOP <= 0 then null
          when aktive.GP_SATS_BELOP < 1 then 1
          else 0
       end as GP_AVKORTET_FLAGG,
       aktive.GP_SATS_BELOP,
       aktive.prorata_teller,
       aktive.prorata_nevner,
       
       -- inntekter og beholdninger
       aktive.BEHOLDNING_PENSJON_BELOP,
       aktive.BEHOLDNING_GARAN_PEN_BELOP,
       aktive.BEHOLDNING_GARAN_TLG_BELOP,
       inntekt.INNTEKT_FPI_BELOP inntekt,
       
       -- inntekter ektefelle/partner/samboer (eps)
       case
          when aktive.k_bor_med_t is not null
                then inntekt_eps.INNTEKT_FPI_BELOP 
          else null
       end as inntekt_eps,
    
       case
          when aktive.k_bor_med_t is not null
               then case 
                      when inntekt_eps.INNTEKT_SFAKPI_BELOP > 0
                           then inntekt_eps.INNTEKT_SFAKPI_BELOP
                           
                      else (nvl(inntekt_eps.INNTEKT_PENSKD_BELOP, 0)
                          + nvl(inntekt_eps.INNTEKT_FKI_BELOP, 0)
                          + nvl(inntekt_eps.INNTEKT_FPI_BELOP, 0)
                          + nvl(inntekt_eps.INNTEKT_PENT_BELOP, 0))
                    end
          else null
       end as eps_aarlig_inntekt,
       --
       
       aktive.brutto - aktive.netto as sum_fradrag,
       coalesce(bi.gp_restpensjon, bi_2016.gp_restpensjon) as gp_restpensjon,
       coalesce(bi.pt_restpensjon, bi_2016.pt_restpensjon) as pt_restpensjon,
       coalesce(bi.tp_restpensjon, bi_2016.tp_restpensjon) as tp_restpensjon,
       --
       to_timestamp(to_char(current_timestamp, 'DD.MM.YYYY HH24.MI.SS,FF9')) as kjoretidspunkt
  FROM aktive 
       
       --- Personinfo  ---------------------------
--       left outer join pen.t_person person on person.person_id = aktive.person_id
       
       --- Beregningsinfo ---------------------------
       left outer join pen.t_beregning_info bi on bi.beregning_info_id = aktive.beregning_info_id 
       left outer join pen.t_beregning_info bi_2016 on bi_2016.beregning_info_id = aktive.beregning_info_id_2016  
       left outer join pen.t_beregning_info bi_2025 on bi_2025.beregning_info_id = aktive.beregning_info_id_2025
       
       --- Inntekter for personen selv ---
       left outer join inntekt on inntekt.person_id = aktive.person_id
                              and inntekt.kravhode_id = aktive.kravhode_id
                              
       --- Inntekter partner/samboer/ektefelle ---
       left outer join inntekt_eps on inntekt_eps.vedtak_id = aktive.vedtak_id
                                          
       --- Innom ytelse_komp for å hente mpn_sats. ---                         
       left outer join pen.t_ytelse_komp yk2 on yk2.beregning_info_id = case
                                                                           when aktive.regelverk = 'N_REG_G_N_OPPTJ' then bi_2025.beregning_info_id
                                                                           when aktive.regelverk <> 'G_REG' then bi.beregning_info_id
                                                                        end
                                            and yk2.k_ytelse_komp_t = 'MIN_NIVA_TILL_INDV'
                                            and yk2.bruk = '1'
                                            and yk2.opphort = '0'     
       left outer join pen.t_min_pen_niva mpn3 on mpn3.min_pen_niva_id = yk2.min_pen_niva_id
       ---
