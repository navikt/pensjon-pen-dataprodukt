{{
  config(
    materialized = 'incremental',
    )
}}

--create or replace view vw_ufore_i_pesys_current as
-------------------------------------------------------------------------------
-- Eirik Grønli
-- 20. april 2026
--
-- Datauttrekk for utføretrygd.
-- Basert på "Monstermappinga" i dagens DVH.
-------------------------------------------------------------------------------
-- Endret: 10. juni 2026
--         Fiksa på etteroppgjør. Joiner på sak_id.
-- Endret: 24. juli 2026
--         Mange nye felter etter siste kravspec.
-------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- Starter først med definisjoner av høvelige CTEs (Common Table Expressions).
------------------------------------------------------------------------------
with 

-------------------------------------------------------
-- CTE for aktive vedtak.
-------------------------------------------------------
vedtak as (
  select v.vedtak_id,
         v.sak_id,
         v.kravhode_id,
         v.k_sak_t,
         v.k_vedtak_t,
         v.k_vedtak_s,
         v.person_id,
         v.DATO_VIRK_FOM,
         v.dato_lopende_fom,
         v.dato_lopende_tom
    from pen.t_vedtak v
   where v.k_sak_t = 'UFOREP'
     and v.dato_lopende_fom <= {{ periode_sluttdato(var("periode")) }}
     and (v.dato_lopende_tom is null or v.dato_lopende_tom >= trunc({{ periode_sluttdato(var("periode")) }}))
     and v.k_vedtak_t in ('ENDRING', 'FORGANG', 'GOMR', 'SAMMENSTOT', 'OPPHOR', 'REGULERING')
     and v.k_vedtak_s in ('IVERKS', 'STOPPES', 'STOPPET', 'REAK')
),

-----------------------------------------------------------
-- CTE for beløp.
-- Akkumulerer/transponerer opp fra YTELSE_KOMP.
-----------------------------------------------------------
yk_bres as
(select v.vedtak_id,
        v.sak_id,
        v.kravhode_id,
        puu.pen_under_utbet_id as under_utbet_id,
        --
        max(puu.total_belop_netto) as sum_netto,
        --
        max(puu.total_belop_brutto) as sum_brutto,
        --
        sum(case when k_ytelse_komp_t = 'UT_SP' and OPPHORT = '0' then yk.netto end) AS UT_SP_NETTO,
        --  
        sum(case when k_ytelse_komp_t = 'UT_TFB' and OPPHORT = '0' then yk.netto end) AS UT_TFB_NETTO,
        --  
        sum(case when k_ytelse_komp_t = 'UT_TSB' and OPPHORT = '0' then yk.netto end) AS UT_TSB_NETTO,
        --  
        sum(case when k_ytelse_komp_t = 'UT_GT_NORDISK' and OPPHORT = '0' then yk.netto end) AS UT_GT_NORDISK_NETTO,
        --  
        sum(case when k_ytelse_komp_t = 'UT_AAP' and OPPHORT = '0' then yk.netto end) AS UT_AAP_NETTO,
        --  
        sum(case when k_ytelse_komp_t = 'UT_ET' and OPPHORT = '0' then yk.netto end) AS UT_ET_NETTO,
        --  
        sum(case when k_ytelse_komp_t = 'UT_GJT' and OPPHORT = '0' then yk.netto end) AS UT_GJT_NETTO,
        --  
        sum(case when k_ytelse_komp_t = 'UT_FAST_UTGIFT_T' and OPPHORT = '0' then yk.netto end) AS UT_FAST_UTG_INST_NETTO,
        --  
        sum(case when k_ytelse_komp_t = 'UT_ORDINER' and OPPHORT = '0' then yk.netto end) AS UFOR_SUM_UT_ORD_NETTO,
        --  
        sum(case when k_ytelse_komp_t = 'UT_TFB' and OPPHORT = '0' then yk.BLP_FRATRKT_ANNEN_FORELD_INNT end) AS BLP_FRATRKT_ANNEN_FORELD_INNT, 
        --
        sum(case 
               when k_ytelse_komp_t in ('UT_AAP', 'UT_ET', 'UT_GJT', 'UT_GT_NORDISK',
                                        'UT_SP', 'UT_TFB', 'UT_TSB', 'UT_FAST_UTGIFT_T')  and OPPHORT = '0' 
                     then yk.netto 
               else 0 
            end) as UFOR_SUM_TILLEGG_NETTO,
        --
        max(case when k_ytelse_komp_t = 'UT_TFB' and OPPHORT = '0' then ANTALL_BARN else 0 end) as BARN_UNDER_18_FELLES_ANTALL,
        -- 
        sum(case when k_ytelse_komp_t = 'UT_TFB' and OPPHORT = '0' then (nvl(yk.SAML_INNTEKT_AVKORT,0) - nvl(yk.BRUKER_INNTEKT_AVKORT,0)) else 0 end) as EPS_ARLIG_INNTEKT_BELOP,
        --  
        max(case when k_ytelse_komp_t = 'UT_ORDINER' and OPPHORT = '0' then egen.arsbelop end) AS egenopptjening_arsbelop,
        
        -- Avkort_Info ------------------------------------------------------------------------------------------------------------ 
        max(case when k_ytelse_komp_t = 'UT_ORDINER' and OPPHORT = '0' then a.utbetalingsgrad end) AS utbetalingsgrad,
        --  
        max(case when k_ytelse_komp_t = 'UT_ORDINER' and OPPHORT = '0' then a.oifu end) AS oifu,
        --  
        max(case when k_ytelse_komp_t = 'UT_ORDINER' and OPPHORT = '0' then a.oieu end) AS oieu,
        --  
        max(case when k_ytelse_komp_t = 'UT_ORDINER' and OPPHORT = '0' then a.inntektsgrense end) AS inntektsgrense,
        --  
        max(case when k_ytelse_komp_t = 'UT_ORDINER' and OPPHORT = '0' then a.belopsgrense end) AS belopsgrense,
        --  
        max(case when k_ytelse_komp_t = 'UT_ORDINER' and OPPHORT = '0' then a.inntektstak end) AS inntektstak,
        --  
        max(case when k_ytelse_komp_t = 'UT_ORDINER' and OPPHORT = '0' then a.forventet_inntekt end) AS forventet_inntekt,
        --  
        max(case when k_ytelse_komp_t = 'UT_ORDINER' and OPPHORT = '0' then a.inntektsgrense_neste_ar end) AS inntektsgrense_neste_aar,
        --  
        max(case when k_ytelse_komp_t = 'UT_ORDINER' and OPPHORT = '0' then a.avkortingsbelop_per_ar end) AS avkortet_per_aar,
        --  
        max(case when k_ytelse_komp_t = 'UT_ORDINER' and OPPHORT = '0' then a.rest_til_utbetaling end) AS rest_til_utbetaling,
        --  
        max(case when k_ytelse_komp_t = 'UT_ORDINER' and OPPHORT = '0' then a.differansebelop end) AS differansebelop,
        --  
        max(case when k_ytelse_komp_t = 'UT_ORDINER' and OPPHORT = '0' then a.ugradert_brutto_per_ar end) AS ugradert_brutto_per_ar,
        --  
        max(case when k_ytelse_komp_t = 'UT_ORDINER' and OPPHORT = '0' then a.kompensasjonsgrad end) AS kompensasjonsgrad,
        
        -- Reduksjonsinformasjon ----------------------------------------------------------------------------------------------------
        max(case when k_ytelse_komp_t = 'UT_TSB' then nullif(nvl(ri.SUM_BRUTTO_FOR_REDUKSJON_BT,0) - nvl(ri.SUM_BRUTTO_ETTER_REDUKSJON_BT,0),0) end) as UTBT_SB_REDUKSJON_AR_BELOP,
        --
        max(case when k_ytelse_komp_t = 'UT_TFB' then nullif(nvl(ri.SUM_BRUTTO_FOR_REDUKSJON_BT,0) - nvl(ri.SUM_BRUTTO_ETTER_REDUKSJON_BT,0),0) end) as UTBT_FB_REDUKSJON_AR_BELOP,
        --
        max(case when k_ytelse_komp_t = 'UT_TFB' then ri.SUM_BRUTTO_FOR_REDUKSJON_BT end) as UTBT_FB_BRUTTO_FR_AR_BELOP,
        --
        max(case when k_ytelse_komp_t = 'UT_TSB' then ri.SUM_BRUTTO_FOR_REDUKSJON_BT end) as UTBT_SB_BRUTTO_FR_AR_BELOP,
        --
        max(case when k_ytelse_komp_t in ('UT_TFB', 'UT_TSB') then ri.PROSENTSATS_OIFU_FOR_TAK end) as UTBT_OIFU_FOR_TAK_PROSENT,
        -----------------------------------------------------------------------------------------------------------------------------
        
        max(case when k_ytelse_komp_t = 'UT_TSB' and OPPHORT = '0' then yk.netto_per_ar end) as UTBT_SB_NTO_AR_BELOP,
        --
        max(case when k_ytelse_komp_t = 'UT_TFB' and OPPHORT = '0' then yk.netto_per_ar end) as UTBT_FB_NTO_AR_BELOP,
        --
        max(case when k_ytelse_komp_t = 'UT_TFB' and OPPHORT = '0' then yk.brutto_per_ar end) as UTBT_FB_BRUTTO_AR_BELOP,
        --
        max(case when k_ytelse_komp_t = 'UT_TSB' and OPPHORT = '0' then yk.brutto_per_ar end) as UTBT_SB_BRUTTO_AR_BELOP,
        --
        max(case when k_ytelse_komp_t = 'UT_TSB' and OPPHORT = '0' then yk.fribelop end) as UTBT_SB_FRI_AR_BELOP,
        --
        max(case when k_ytelse_komp_t = 'UT_TFB' and OPPHORT = '0' then yk.fribelop end) as UTBT_FB_FRI_AR_BELOP,
        --
        max(case when k_ytelse_komp_t = 'UT_TFB' and OPPHORT = '0' then yk.AVKORTNINGSBELOP_PER_AR end) as UTBT_FB_AVKORTNING_AR_BELOP,
        --
        max(case when k_ytelse_komp_t = 'UT_TSB' and OPPHORT = '0' then yk.AVKORTNINGSBELOP_PER_AR end) as UTBT_SB_AVKORTNING_AR_BELOP,
        --
        sum(case when k_ytelse_komp_t = 'UT_TFB' and OPPHORT = '0' then yk.INNTEKT_ANNEN_FORELDER end) as INNTEKT_ANNEN_FORELDER,
        --
        sum(case when k_ytelse_komp_t = 'UT_TFB' and OPPHORT = '0' then yk.BRUKER_INNTEKT_AVKORT else 0 end) as BRUKER_INNTEKT_AVKORT
        --
   from vedtak v
        inner join pen.t_beregning_res br on br.vedtak_id = v.vedtak_id
                                         and br.dato_virk_fom <= {{ periode_sluttdato(var("periode")) }}
                                         and (br.dato_virk_tom is null or br.dato_virk_tom >= trunc({{ periode_sluttdato(var("periode")) }}))
        inner join PEN.t_pen_under_utbet puu on puu.pen_under_utbet_id = br.pen_under_utbet_id 
        inner join pen.t_ytelse_komp yk on yk.pen_under_utbet_id = puu.pen_under_utbet_id
                                       and yk.bruk = '1'
--                                       and yk.opphort = '0'
        left outer join pen.t_avkort_info a on a.avkort_info_id = yk.avkort_info_id  
        left outer join pen.T_EGENOPPTJN_UT egen on egen.EGENOPPTJN_UT_ID = yk.EGENOPPTJN_UT_ID
        left outer join pen.T_REDUKSJONSINFORMASJON ri on ri.REDUKSJONSINFORMASJON_ID = yk.REDUKSJONSINFORMASJON_ID
                                                      and yk.k_ytelse_komp_t in ('UT_TSB','UT_TFB')
  where yk.bruk=1
  group by v.vedtak_id, v.sak_id, v.kravhode_id, puu.pen_under_utbet_id
),

--------------------------------------------------------------------
-- CTE for diverse datoer.
-- Akkumulerer fra UFORE_HISTORIKK og UFORE_PERIODE.
--------------------------------------------------------------------
ufor_hist as (
  select v.person_id as person_id,
         min(up.dato_ufore_fom) as FORSTE_UFORE_DATO,
         min(up.dato_virk) as FORSTE_VIRKNING_UFORE_DATO,
         max(up.dato_ufore_fom) AS GJELDENDE_UFORE_DATO
    from vedtak v
         inner join pen.t_ufore_historik uh on uh.person_id = v.person_id
         inner join pen.t_ufore_periode up on uh.UFORE_HISTORIK_id = up.UFORE_HISTORIK_id
   where up.k_ufore_t <> 'VIRK_IKKE_UFOR' 
   group by v.person_id
),

-----------------------------------------------------------------------------
-- CTE for eventuelle etteroppgjør.
-- Akkumulerer/transponerer fra t_eo_ut_historik, grupperer på sak + vedtak.
-- Vi må ha med fra T_PERSON spesielt for de sakene som ikke har noen makker
-- i de vanlige uførevedtakene i CTE-en vedtak ovenfor.
-----------------------------------------------------------------------------
etteroppgjor as (
   select akk.sak_id, 
          akk.vedtak_id,
          person.fnr_fk as fnr_fk,
          person.dato_fodsel as dato_fodsel,
          akk.etterbetalt_belop, 
          akk.tilbakekrevd_belop,
          akk.etteroppgjor_aar,
          akk.revurdering_flagg
     from (
           select eoh.sak_id, eoh.vedtak_id,
                  --
                  max(case
                      when k_ut_eo_resultat = 'ETTERBET' then abs(eoh.avviksbelop)
                      else null
                  end) as etterbetalt_belop,
                  --
                  max(case
                      when k_ut_eo_resultat = 'TILBAKEKR' then abs(eoh.avviksbelop)
                      else null
                  end) as tilbakekrevd_belop,
                  --
                  max(eoh.ar) as etteroppgjor_aar,
                  max(to_number(eoh.er_revurdering)) as revurdering_flagg
             from pen.t_eo_ut_historik eoh
            where eoh.er_gyldig = 1
              and to_char(eoh.dato_endret,'YYYYMM') 
                         = to_char({{ periode_sluttdato(var("periode")) }}, 'YYYYMM') 
              and eoh.k_ut_eo_resultat in ('TILBAKEKR', 'ETTERBET')
            group by eoh.sak_id, eoh.vedtak_id 
          ) akk
          inner join pen.t_vedtak vv on vv.vedtak_id = akk.vedtak_id
          inner join pen.t_person person on person.person_id = vv.person_id

),

-----------------------------------------------------------
-- CTE for vilkaar_vedtak og beregning_vilkaar.
-- Henter inntekt etter ufør, og yrkesskaderettflagg.
-----------------------------------------------------------
vilkaar as 
( select vedtak_id,
         max(case when K_BEREGNING_VILKAR_T = 'INNTEKT_FOR_UFORHET' then inntekt end) as inntekt_foer,
         max(case when K_BEREGNING_VILKAR_T = 'INNTEKT_ETTER_UFOR' then inntekt end) as inntekt_etter,
         max(CASE WHEN K_BEREGNING_VILKAR_T = 'YRKESSKADEGRAD' and nvl(GRAD,0) > 0 THEN 1 ELSE 0 END) as YRKESSKADE_RETT_FLAGG
    from ( SELECT v.vedtak_id,
                  PVV.vilkar_vedtak_id, 
                  bv.K_BEREGNING_VILKAR_T,
                  bv.inntekt,
                  bv.dato_opprettet,
                  bv.grad,
                  pvv.dato_virk_fom, 
                  pvv.dato_virk_tom,
                  row_number() over(partition by pvv.vedtak_id, K_BEREGNING_VILKAR_T order by pvv.vilkar_vedtak_id) as rn
             FROM vedtak v
                  left outer join PEN.T_VILKAR_VEDTAK PVV
                      on pvv.vedtak_id = v.vedtak_id
                     and pvv.k_kravlinje_t = 'UT'
                  left outer join PEN.T_BEREGNING_VILKAR BV
                       on bv.vilkar_vedtak_id = pvv.vilkar_vedtak_id
            WHERE pvv.k_vilkar_resul_t = 'INNV'
              AND pvv.dato_virk_fom <= {{ periode_sluttdato(var("periode")) }}  
              AND (pvv.dato_virk_tom >= {{ periode_sluttdato(var("periode")) }} 
                   OR pvv.dato_virk_tom IS NULL)
              and bv.K_BEREGNING_VILKAR_T in ('INNTEKT_ETTER_UFOR', 'INNTEKT_FOR_UFORHET', 'YRKESSKADEGRAD')
         )
    where rn = 1
  group by vedtak_id
),

-----------------------------------
-- CTE for diverse inntektstyper.
-----------------------------------
inntekt as (
   select person_id, kravhode_id,
          max(case when k_inntekt_t='FORINTARB' then belop else 0 end) as INNTEKT_FORINTARB_BELOP,
          max(case when k_inntekt_t='FORINTNAE' then belop else 0 end) as INNTEKT_FORINTNAE_BELOP,
          max(case when k_inntekt_t='FORINTUTL' then belop else 0 end) as INNTEKT_FORINTUTL_BELOP
     from (select DISTINCT 
                  person_id, 
                  kravhode_id, 
                  k_inntekt_t,
                  LAST_VALUE(BELOP) OVER (PARTITION BY  person_id, kravhode_id, k_inntekt_t 
                                          ORDER BY dato_fom ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as belop,
                  LAST_VALUE(K_GRUNNLAG_KILDE) OVER (PARTITION BY  person_id, kravhode_id, k_inntekt_t 
                                                          ORDER BY dato_fom ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as K_GRUNNLAG_KILDE
             from (select pg.person_id, 
                          pg.kravhode_id, 
                          i.BELOP, 
                          i.DATO_FOM, 
                          i.k_inntekt_t, 
                          i.K_GRUNNLAG_KILDE 
                     from vedtak v 
                          inner join pen.t_person_grunnlag pg 
                               on pg.person_id=v.person_id 
                              and pg.kravhode_id=v.kravhode_id
                          inner join pen.t_inntekt i
                               on pg.person_grunnlag_id=i.person_grunnlag_id
                    where i.bruk = 1 
                      and (i.DATO_FOM <= {{ periode_sluttdato(var("periode")) }} and ( {{ periode_sluttdato(var("periode")) }} <= i.DATO_TOM or i.DATO_TOM is null))
                   )
           ) inntekt
   group by person_id, kravhode_id
),

--------------------------------------------------------------------
-- CTE for inntektsendringer for en sak i perioden.
-- Disse tallene akkumuleres i ETL-jobben som løpende hittil i år.
--------------------------------------------------------------------
inntektsendring_i_perioden as (
    select sak_id, 
           person_id, 
           SUM(CASE WHEN k_grunnlag_kilde = 'SAKSB' THEN 1 ELSE null END) AS ANTALL_IE_VEDTAK_SAKSB,
           SUM(CASE WHEN k_grunnlag_kilde = 'BRUKER_OPP' THEN 1 ELSE null END) AS ANTALL_IE_VEDTAK_BRUKER,
           SUM(CASE WHEN k_grunnlag_kilde = 'PROSESS' THEN 1 ELSE null END) AS ANTALL_IE_VEDTAK_PROSESS
      from (select distinct 
                   v.sak_id, 
                   v.vedtak_id, 
                   v.person_id, 
                   i.k_grunnlag_kilde,
                   
                   -- De følgende last_value() brukes ikke per i dag, så de er for så vidt unødvendige.
                   LAST_VALUE(i.k_grunnlag_kilde) 
                        OVER (PARTITION BY  v.sak_id, v.person_id, v.vedtak_id 
                              ORDER BY i.dato_endret ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as siste_kilde,
                   LAST_VALUE(i.k_inntekt_t) 
                        OVER (PARTITION BY  v.sak_id, v.person_id, v.vedtak_id 
                              ORDER BY i.dato_endret ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as siste_inntektstype,
                   LAST_VALUE(i.dato_endret) 
                        OVER (PARTITION BY  v.sak_id, v.person_id, v.vedtak_id 
                              ORDER BY i.dato_endret ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as siste_dato,
                   LAST_VALUE(i.belop) 
                        OVER (PARTITION BY  v.sak_id, v.person_id, v.vedtak_id 
                              ORDER BY i.dato_endret ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as siste_belop
              from pen.t_vedtak v
                   inner join pen.t_kravhode k 
                        on k.kravhode_id = v.kravhode_id
                   inner join pen.t_person_grunnlag pg 
                        on pg.person_id = v.person_id 
                       and pg.kravhode_id = v.kravhode_id
                   inner join pen.t_inntekt i
                        on pg.person_grunnlag_id = i.person_grunnlag_id
             where v.k_sak_t = 'UFOREP' 
               and k.K_KRAV_GJELDER = 'INNT_E' 
               and i.BRUK = 1 
               and i.k_inntekt_t IN ('FORINTARB', 'FORINTNAE', 'FORINTUTL') 
               and TO_CHAR(v.DATO_LOPENDE_FOM, 'YYYYMM')  = to_char({{ periode_sluttdato(var("periode")) }}, 'YYYYMM') 
               --
               and to_char({{ periode_sluttdato(var("periode")) }}, 'YYYYMM') 
                   BETWEEN TO_CHAR(i.DATO_FOM, 'YYYYMM') 
                       and nvl(TO_CHAR(i.DATO_TOM, 'YYYYMM'), '209901')
               --
               and i.k_grunnlag_kilde in ('SAKSB', 'BRUKER_OPP', 'PROSESS')   
           ) inntekt_endring
    group by sak_id, person_id
),

-----------------------------------------------------
-- CTE som forener alle ovenstående CTE-er.
-----------------------------------------------------
siste as (
    select 
            to_date({{ var("periode") }}, 'YYYYMM') as periode,
            v.vedtak_id,
            coalesce(v.sak_id, etter.sak_id) as sak_id,
            v.kravhode_id,
            nvl(v.k_sak_t, 'UFOREP') as k_sak_t,
            --
            case 
                when kh.K_KRAV_VELG_T = 'VARIG' then 1
                when v.vedtak_id is not null then 0
                -- null hvis kun etteroppgjør
                else null
            end as varig_vedtak,
            --
            case 
                when kh.K_KRAV_VELG_T = 'FORELOPIG' then 1 
                when v.vedtak_id is not null then 0
                -- null hvis kun etteroppgjør
                else null
            end as forelopig_vedtak,
            --
            v.dato_lopende_fom,
            v.dato_lopende_tom,
            yk.sum_netto as total_belop_netto,
            yk.sum_brutto as total_belop_brutto,
            --
            case 
                when v.vedtak_id is not null
                    then nvl(UFOR_SUM_UT_ORD_NETTO,0)
                        + nvl(UT_GJT_NETTO,0)
                else null
            end as FOLKETRYGD_2016,
            --
            yk.UFOR_SUM_UT_ORD_NETTO,
            yk.UFOR_SUM_TILLEGG_NETTO,
            
            -- Fra reduksjonsinformasjon
            yk.UTBT_SB_REDUKSJON_AR_BELOP,
            yk.UTBT_FB_REDUKSJON_AR_BELOP,
            yk.UTBT_FB_BRUTTO_FR_AR_BELOP,         
            yk.UTBT_SB_BRUTTO_FR_AR_BELOP,  
            yk.UTBT_OIFU_FOR_TAK_PROSENT,
            
            --
            yk.UTBT_SB_NTO_AR_BELOP,         
            yk.UTBT_FB_NTO_AR_BELOP,      
            yk.UTBT_SB_FRI_AR_BELOP,         
            yk.UTBT_FB_FRI_AR_BELOP,         
            yk.UTBT_FB_BRUTTO_AR_BELOP,        
            yk.UTBT_SB_BRUTTO_AR_BELOP,         
            yk.UTBT_FB_AVKORTNING_AR_BELOP,         
            yk.UTBT_SB_AVKORTNING_AR_BELOP,  
            yk.INNTEKT_ANNEN_FORELDER,
            yk.BLP_FRATRKT_ANNEN_FORELD_INNT,
            yk.BARN_UNDER_18_FELLES_ANTALL,
            yk.BRUKER_INNTEKT_AVKORT,
            yk.EPS_ARLIG_INNTEKT_BELOP,
            --
            yk.UT_ET_NETTO,
            yk.UT_SP_NETTO,
            yk.UT_TFB_NETTO,
            yk.UT_TSB_NETTO,
            yk.UT_GJT_NETTO,
            yk.UT_FAST_UTG_INST_NETTO,
            yk.UT_GT_NORDISK_NETTO,
            yk.UT_AAP_NETTO,
            --
            yk.utbetalingsgrad,
            floor(months_between({{ periode_sluttdato(var("periode")) }}, p.dato_fodsel)/12) as alder,
            floor(months_between(LAST_DAY(uh.FORSTE_UFORE_DATO), p.dato_fodsel)/12) as alder_ufor,
            floor(months_between({{ periode_sluttdato(var("periode")) }}, p.dato_fodsel)/12)
            + case 
                    when extract(month from p.dato_fodsel) = extract(month from {{ periode_sluttdato(var("periode")) }}) 
                    and extract(day from p.dato_fodsel) > extract(day from {{ periode_sluttdato(var("periode")) }})
                        then 1
                    else 0
                end alder_fix,
            --
            coalesce(p.dato_fodsel, etter.dato_fodsel) as dato_fodsel,
            uh.FORSTE_UFORE_DATO,
            uh.FORSTE_VIRKNING_UFORE_DATO,
            uh.GJELDENDE_UFORE_DATO,
            --
            case 
                when kh.KONVERTERINGSGRUNNLAG_UT_ID is not null then 1 
                when v.vedtak_id is not null then 0
                -- null hvis kun etteroppgjør
                else null
            end as konvertert,
            --
            b2011.uforegrad,
            b2011.MOTTAR_MINSTEYTELSE,
            smy.ung_ufor_benyttet as ung_ufor_anv_flagg,
            smy.UNG_UFOR_OPPFYLT as ung_ufor_rett_flagg,
            vk.YRKESSKADE_RETT_FLAGG,
            --
            CASE 
                when b2011.YRKESSKADEGRAD IS NOT NULL AND b2011.YRKESSKADEGRAD > 0
                    then 1 
                when v.vedtak_id is not null then 0
                -- null hvis kun etteroppgjør
                ELSE null
            END AS YRKESSKADE_ANV_FLAGG,
            --
            case 
                when b2011.MOTTAR_MINSTEYTELSE = 1 and  smy.ung_ufor_benyttet = 0
                    then 1
                when v.vedtak_id is not null then 0
                -- null hvis kun etteroppgjør
                else null
            end as MOTTAR_MIN_UFORE_NIVA_PEN_FLAGG,
            --
            yk.inntektsgrense,
            yk.belopsgrense,
            yk.inntektstak,
            yk.egenopptjening_arsbelop,
            yk.oifu,
            yk.oieu,
            yk.forventet_inntekt,
            yk.inntektsgrense_neste_aar,
            yk.avkortet_per_aar,
            yk.rest_til_utbetaling,
            to_number(yk.differansebelop) as differansebelop,
            yk.ugradert_brutto_per_ar,
            round(yk.kompensasjonsgrad,2) as kompensasjonsgrad,
            vk.inntekt_foer,
            vk.inntekt_etter,
            i.INNTEKT_FORINTARB_BELOP as FORINTARB,
            i.INNTEKT_FORINTNAE_BELOP as FORINTNAE,
            i.INNTEKT_FORINTUTL_BELOP as FORINTUTL,
            --
            round(to_number(
                    case 
                        when nvl(brok.nevner,0) <> 0  then brok.teller / brok.nevner
                        else null
                    end), 10) 
                as ufor_prorata,
            --
            coalesce(p.fnr_fk, etter.fnr_fk) as persnr,
            v.k_vedtak_t,
            v.k_vedtak_s,
            --
            smy.sats as sats_minsteytelse,
            smy.k_minsteytelseniva as minsteytelse_niva,
            smy.k_bor_med_t as bostatus,
            --
            b2011.tt_anv as trygdetid,
            v.DATO_VIRK_FOM as GJELDENDE_VIRKNING_UFORE_DATO,
            br.k_bor_med_t as bostatus_sats,
            
            -- Flagger for om det er regulært vedtak eller etteroppgjør.
            -- Begge flaggene kan ha lik verdi.
            case when v.vedtak_id is not null then 1 else 0 end as vedtak_flagg,
            case when etter.vedtak_id is not null then 1 else 0 end as eo_flagg,
            --
            ieip.ANTALL_IE_VEDTAK_SAKSB,
            ieip.ANTALL_IE_VEDTAK_BRUKER,
            ieip.ANTALL_IE_VEDTAK_PROSESS,    
            
            -- Fra etteroppgjør.
            etter.vedtak_id as eo_vedtak,
            etter.etterbetalt_belop,
            etter.tilbakekrevd_belop,
            etter.etteroppgjor_aar,
            etter.revurdering_flagg,
            --
            sysdate as kjoretidspunkt
            --
        from vedtak v
            inner join yk_bres yk on v.vedtak_id = yk.vedtak_id
            left outer join vilkaar vk on vk.vedtak_id = v.vedtak_id
            left outer join inntekt i on i.person_id = v.person_id
                                    and i.kravhode_id = v.kravhode_id
            left outer join ufor_hist uh on uh.person_id = v.person_id
            left outer join pen.t_kravhode kh on kh.kravhode_id = v.kravhode_id
            left outer join pen.t_beregning_res br on br.vedtak_id = v.vedtak_id
                                                and br.dato_virk_fom <= {{ periode_sluttdato(var("periode")) }}
                                                and (br.dato_virk_tom is null or br.dato_virk_tom >= trunc({{ periode_sluttdato(var("periode")) }}))
            left outer join pen.t_beregning_2011 b2011 on b2011.beregning_2011_id = br.uforetrygd_beregning_id
            left outer join pen.t_minsteytelse my on my.minsteytelse_id = b2011.minsteytelse_id
            left outer join pen.t_sats_minsteytelse smy on smy.sats_minsteytelse_id = my.sats_minsteytelse_id
            left outer join pen.t_brok brok on brok.brok_id = b2011.prorata_brok_id
            left outer join pen.t_person p on p.person_id = v.person_id
            left outer join inntektsendring_i_perioden ieip on ieip.sak_id = v.sak_id 
                                                            and ieip.person_id = v.person_id
    --         left outer join reduksjonsinformasjon ri on ri.vedtak_id = v.vedtak_id
            full join etteroppgjor etter on etter.sak_id = v.sak_id
)

select 
    periode,
    vedtak_id,
    sak_id,
    kravhode_id,
    k_sak_t,
    varig_vedtak,
    forelopig_vedtak,
    dato_lopende_fom,
    dato_lopende_tom,
    total_belop_netto,
    total_belop_brutto,
    folketrygd_2016,
    ufor_sum_ut_ord_netto,
    ufor_sum_tillegg_netto,
    utbt_sb_reduksjon_ar_belop,
    utbt_fb_reduksjon_ar_belop,
    utbt_fb_brutto_fr_ar_belop,
    utbt_sb_brutto_fr_ar_belop,
    utbt_oifu_for_tak_prosent,
    utbt_sb_nto_ar_belop,
    utbt_fb_nto_ar_belop,
    utbt_sb_fri_ar_belop,
    utbt_fb_fri_ar_belop,
    utbt_fb_brutto_ar_belop,
    utbt_sb_brutto_ar_belop,
    utbt_fb_avkortning_ar_belop,
    utbt_sb_avkortning_ar_belop,
    inntekt_annen_forelder,
    blp_fratrkt_annen_foreld_innt,
    barn_under_18_felles_antall,
    bruker_inntekt_avkort,
    eps_arlig_inntekt_belop,
    ut_et_netto,
    ut_sp_netto,
    ut_tfb_netto,
    ut_tsb_netto,
    ut_gjt_netto,
    ut_fast_utg_inst_netto,
    ut_gt_nordisk_netto,
    ut_aap_netto,
    utbetalingsgrad,
    alder,
    alder_ufor,
    alder_fix,
    dato_fodsel,
    forste_ufore_dato,
    forste_virkning_ufore_dato,
    gjeldende_ufore_dato,
    konvertert,
    uforegrad,
    mottar_minsteytelse,
    ung_ufor_anv_flagg,
    ung_ufor_rett_flagg,
    yrkesskade_rett_flagg,
    yrkesskade_anv_flagg,
    mottar_min_ufore_niva_pen_flagg,
    inntektsgrense,
    belopsgrense,
    inntektstak,
    egenopptjening_arsbelop,
    oifu,
    oieu,
    forventet_inntekt,
    inntektsgrense_neste_aar,
    avkortet_per_aar,
    rest_til_utbetaling,
    differansebelop,
    ugradert_brutto_per_ar,
    kompensasjonsgrad,
    inntekt_foer,
    inntekt_etter,
    forintarb,
    forintnae,
    forintutl,
    ufor_prorata,
    persnr,
    k_vedtak_t,
    k_vedtak_s,
    sats_minsteytelse,
    minsteytelse_niva,
    bostatus,
    trygdetid,
    gjeldende_virkning_ufore_dato,
    bostatus_sats,
    vedtak_flagg,
    eo_flagg,
    antall_ie_vedtak_saksb,
    antall_ie_vedtak_bruker,
    antall_ie_vedtak_prosess,
    eo_vedtak,
    etterbetalt_belop,
    tilbakekrevd_belop,
    etteroppgjor_aar,
    revurdering_flagg,
    kjoretidspunkt
from siste 
where 1=1
{% if is_incremental() %}
    and to_date({{ var("periode") }}, 'YYYYMM') not in (select distinct periode from {{ this }}) -- noqa
{% endif %}
