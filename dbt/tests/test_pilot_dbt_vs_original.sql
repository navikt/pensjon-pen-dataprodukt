{% set cols = [ "grein", "sakstype", "uttaksgrad", "netto", "gp_netto", "tp_netto", "pt_netto", "st_netto", "et_netto", "ip_netto", "gap_netto", "gjt_netto", "gjt_k19_netto", "skjermt_netto", "saerkull_netto", "mpn_sstot_netto", "mpn_indiv_netto", "barn_felles_netto", "ufor_sum_ut_ord_netto", "ap_kap19_uten_gjr_bel", "ap_kap19_med_gjr_bel", "minste_pen_niva", "afp_lopph_flagg", "afp_finans_flagg", "afp_lopph_ytelse_flagg", "afp_privat_flagg", "gjenlevrett_anv", "red_pga_inst_opph_flagg", "tt_anvendt_kap19_antall", "tt_anvendt_kap20_antall", "minstepensjon", "anvendt_yrkesskade_flagg", "aldersytelseflagg", "afp_ordning", "regelverk"] %}

with

d as (
    select
        coalesce(sak_id, 0) as sak_id
  , coalesce(vedtak_id, 0) as vedtak_id
  , coalesce(kravhode_id, 0) as kravhode_id
  --, coalesce(person_id, 0) as person_id
  , coalesce(grein, '0') as grein
  , coalesce(sakstype, '0') as sakstype
  , coalesce(uttaksgrad, 0) as uttaksgrad
  , coalesce(netto, 0) as netto
  , coalesce(gp_netto, 0) as gp_netto
  , coalesce(tp_netto, 0) as tp_netto
  , coalesce(pt_netto, 0) as pt_netto
  , coalesce(st_netto, 0) as st_netto
  , coalesce(et_netto, 0) as et_netto
  , coalesce(ip_netto, 0) as ip_netto
  , coalesce(gap_netto, 0) as gap_netto
  , coalesce(gjt_netto, 0) as gjt_netto
  , coalesce(gjt_k19_netto, 0) as gjt_k19_netto
  , coalesce(skjermt_netto, 0) as skjermt_netto
  , coalesce(saerkull_netto, 0) as saerkull_netto
  , coalesce(mpn_sstot_netto, 0) as mpn_sstot_netto
  , coalesce(mpn_indiv_netto, 0) as mpn_indiv_netto
  , coalesce(barn_felles_netto, 0) as barn_felles_netto
  , coalesce(ufor_sum_ut_ord_netto, 0) as ufor_sum_ut_ord_netto
  , coalesce(ap_kap19_uten_gjr_bel, 0) as ap_kap19_uten_gjr_bel
  , coalesce(ap_kap19_med_gjr_bel, 0) as ap_kap19_med_gjr_bel
  , coalesce(minste_pen_niva, '0') as minste_pen_niva
  , coalesce(afp_lopph_flagg, 0) as afp_lopph_flagg
  , coalesce(afp_finans_flagg, 0) as afp_finans_flagg
  , coalesce(afp_lopph_ytelse_flagg, 0) as afp_lopph_ytelse_flagg
--   , coalesce(overgangsstonad, '0') as overgangsstonad
  , coalesce(afp_privat_flagg, 0) as afp_privat_flagg
  , coalesce(gjenlevrett_anv, '0') as gjenlevrett_anv
  , coalesce(red_pga_inst_opph_flagg, 0) as red_pga_inst_opph_flagg
  , coalesce(tt_anvendt_kap19_antall, 0) as tt_anvendt_kap19_antall
  , coalesce(tt_anvendt_kap20_antall, 0) as tt_anvendt_kap20_antall
  , coalesce(minstepensjon, 0) as minstepensjon
  , coalesce(anvendt_yrkesskade_flagg, 0) as anvendt_yrkesskade_flagg
  , coalesce(aldersytelseflagg, 0) as aldersytelseflagg
  , coalesce(afp_ordning, '0') as afp_ordning
  , coalesce(regelverk, '0') as regelverk
    from {{ ref("sql_pilot_dbt") }}
),

o as (
    select
        coalesce(sak_id, 0) as sak_id
  , coalesce(vedtak_id, 0) as vedtak_id
  , coalesce(kravhode_id, 0) as kravhode_id
  --, coalesce(person_id, 0) as person_id
  , coalesce(grein, '0') as grein
  , coalesce(sakstype, '0') as sakstype
  , coalesce(uttaksgrad, 0) as uttaksgrad
  , coalesce(netto, 0) as netto
  , coalesce(gp_netto, 0) as gp_netto
  , coalesce(tp_netto, 0) as tp_netto
  , coalesce(pt_netto, 0) as pt_netto
  , coalesce(st_netto, 0) as st_netto
  , coalesce(et_netto, 0) as et_netto
  , coalesce(ip_netto, 0) as ip_netto
  , coalesce(gap_netto, 0) as gap_netto
  , coalesce(gjt_netto, 0) as gjt_netto
  , coalesce(gjt_k19_netto, 0) as gjt_k19_netto
  , coalesce(skjermt_netto, 0) as skjermt_netto
  , coalesce(saerkull_netto, 0) as saerkull_netto
  , coalesce(mpn_sstot_netto, 0) as mpn_sstot_netto
  , coalesce(mpn_indiv_netto, 0) as mpn_indiv_netto
  , coalesce(barn_felles_netto, 0) as barn_felles_netto
  , coalesce(ufor_sum_ut_ord_netto, 0) as ufor_sum_ut_ord_netto
  , coalesce(ap_kap19_uten_gjr_bel, 0) as ap_kap19_uten_gjr_bel
  , coalesce(ap_kap19_med_gjr_bel, 0) as ap_kap19_med_gjr_bel
  , coalesce(minste_pen_niva, '0') as minste_pen_niva
  , coalesce(afp_lopph_flagg, 0) as afp_lopph_flagg
  , coalesce(afp_finans_flagg, 0) as afp_finans_flagg
  , coalesce(afp_lopph_ytelse_flagg, 0) as afp_lopph_ytelse_flagg
-- --   , coalesce(overgangsstonad, '0') as overgangsstonad
  , coalesce(afp_privat_flagg, 0) as afp_privat_flagg
  , coalesce(gjenlevrett_anv, '0') as gjenlevrett_anv
  , coalesce(red_pga_inst_opph_flagg, 0) as red_pga_inst_opph_flagg
  , coalesce(tt_anvendt_kap19_antall, 0) as tt_anvendt_kap19_antall
  , coalesce(tt_anvendt_kap20_antall, 0) as tt_anvendt_kap20_antall
  , coalesce(minstepensjon, 0) as minstepensjon
  , coalesce(anvendt_yrkesskade_flagg, 0) as anvendt_yrkesskade_flagg
  , coalesce(aldersytelseflagg, 0) as aldersytelseflagg
  , coalesce(afp_ordning, '0') as afp_ordning
  , coalesce(regelverk, '0') as regelverk
    from {{ ref("sql_pilot_original") }}
),

{% for col in cols %}
    diff_{{ col }} as (
        select
            d.sak_id,
            d.vedtak_id,
            d.kravhode_id,
            d.{{ col }} as col_test
        from d
        minus
        select
            o.sak_id,
            o.vedtak_id,
            o.kravhode_id,
            o.{{ col }} as col_test
        from o
    ),
{% endfor %}

final as (
    {% for col in cols %}
        select
        '{{ col }}' as col,
        count(*) as count
        from diff_{{ col }}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select * from final
