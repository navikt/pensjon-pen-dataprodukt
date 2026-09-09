{{
  config(
    materialized = 'table',
    tags = ['analyse'],
    )
}}

with

lopende_vedtak as (
    select * from {{ ref('analyse_lopende_vedtak_ufore') }}
),

familie_info as (
    select
        v.*,
        pd.*,
        extract(year from current_date) - extract(year from p.dato_fodsel) as alder,
        row_number() over (partition by pg.person_grunnlag_id order by pd.dato_opprettet desc) as rn
    from lopende_vedtak v
    left join pen.t_person_grunnlag pg
        on
            v.kravhode_id = pg.kravhode_id
    inner join pen.t_person_det pd
        on
            pg.person_grunnlag_id = pd.person_grunnlag_id
            and pd.bruk = '1'
            and pd.rolle_tom is null
    inner join pen.t_person p on pg.person_id = p.person_id
),

familie_info_per_kravhode as (
    select
        sak_id,
        sum(case when k_grnl_rolle_t = 'BARN' then 1 else 0 end) as antall_barn,
        sum(case when k_grnl_rolle_t = 'BARN' and alder < 18 then 1 else 0 end) as antall_barn_under_18,
        max(case when k_grnl_rolle_t in ('EKTEF', 'PARTNER', 'SAMBO') then 1 else 0 end) as har_eps -- Denne kan være 0, selv om søkeren har en aktiv rolle i EPS sin uføresak
    from familie_info
    where rn = 1
    group by sak_id
),

har_eps_med_ut as (
    select
        v.sak_id,
        max(case when pd.k_grnl_rolle_t in ('EKTEF', 'PARTNER', 'SAMBO') then pg.kravhode_id end) as eps_kravhode_id -- Denne kan være 1, selv om søkeren ikke har en EPS med aktiv rolle
    from lopende_vedtak v
    left join pen.t_sak s on v.sak_id = s.sak_id
    left join pen.t_person_grunnlag pg on s.person_id = pg.person_id
    left join pen.t_person_det pd
        on
            pg.person_grunnlag_id = pd.person_grunnlag_id
            and pd.bruk = '1'
            and pd.rolle_tom is null
    group by v.sak_id
),


legg_til_belop as (

    select
        v.sak_id,
        v.vedtak_id,
        v.kravhode_id,
        v.ufore_historik_id,
        v.pen_under_utbet_id,
        v.beregning_res_id,
        v.uforetrygd_beregning_id,
        v.ytelse_komp_id,
        v.avkort_info_id,
        extract(year from current_date) - extract(year from p.dato_fodsel) as alder,
        {{ kjonn_fra_fnr("p.fnr_fk") }} as kjonn,
        case when yk_tfb.netto > 0 or yk_tsb.netto > 0 then 1 else 0 end as barnetillegg_flagg,
        b2011.uforegrad,
        b2011.mottar_minsteytelse,
        cte1.antall_barn,
        cte1.antall_barn_under_18,
        cte1.har_eps,
        cte2.eps_kravhode_id,
        yk.brutto as yk_brutto,
        yk.netto as yk_netto,
        yk.brutto_per_ar as yk_brutto_per_ar,
        yk.netto_per_ar as yk_netto_per_ar,
        yk_tfb.brutto as yk_tfb_brutto,
        yk_tfb.netto as yk_tfb_netto,
        yk_tfb.brutto_per_ar as yk_tfb_brutto_per_ar,
        yk_tfb.netto_per_ar as yk_tfb_netto_per_ar,
        yk_tsb.brutto as yk_tsb_brutto,
        yk_tsb.netto as yk_tsb_netto,
        yk_tsb.brutto_per_ar as yk_tsb_brutto_per_ar,
        yk_tsb.netto_per_ar as yk_tsb_netto_per_ar,
        case when cte1.antall_barn_under_18 > 0 then round(0.15 * 136549 * cte1.antall_barn_under_18) end as bt_brutto_ar,
        case when cte1.antall_barn_under_18 > 0 then round(0.15 * 136549 * cte1.antall_barn_under_18 / 12) end as bt_brutto

    from lopende_vedtak v
    left join familie_info_per_kravhode cte1
        on v.sak_id = cte1.sak_id
    left join har_eps_med_ut cte2
        on v.sak_id = cte2.sak_id
    inner join pen.t_ytelse_komp yk
        on
            v.pen_under_utbet_id = yk.pen_under_utbet_id
            and yk.bruk = '1'
            and yk.k_ytelse_komp_t = 'UT_ORDINER'
    left join pen.t_ytelse_komp yk_tfb
        on
            v.pen_under_utbet_id = yk_tfb.pen_under_utbet_id
            and yk_tfb.k_ytelse_komp_t = 'UT_TFB'
            and yk_tfb.bruk = '1'
    left join pen.t_ytelse_komp yk_tsb
        on
            v.pen_under_utbet_id = yk_tsb.pen_under_utbet_id
            and yk_tsb.k_ytelse_komp_t = 'UT_TSB'
            and yk_tsb.bruk = '1'
    inner join {{ ref("stg_t_sak") }} s on v.sak_id = s.sak_id
    inner join {{ ref("stg_t_person") }} p on s.person_id = p.person_id
    left join pen.t_beregning_2011 b2011 on v.uforetrygd_beregning_id = b2011.beregning_2011_id
),

final as (
    select
        f.*,
        coalesce(f.yk_tfb_netto, 0) + coalesce(f.yk_tsb_netto, 0) + coalesce(f2.yk_tfb_netto, 0) + coalesce(f2.yk_tsb_netto, 0) as husholdning_bt_netto,
        coalesce(f.yk_tfb_brutto, 0) + coalesce(f.yk_tsb_brutto, 0) + coalesce(f2.yk_tfb_brutto, 0) + coalesce(f2.yk_tsb_brutto, 0) as husholdning_bt_brutto,
        -- høringsforslag
        coalesce(f.bt_brutto, 0) + coalesce(f2.bt_brutto, 0) as husholdning_ny_bt_brutto,
        coalesce(f.bt_brutto_ar, 0) + coalesce(f2.bt_brutto_ar, 0) as husholdning_ny_bt_brutto_ar,

        case when f.eps_kravhode_id is not null then 1 else 0 end as eps_med_ut_flagg

    from legg_til_belop f
    left join legg_til_belop f2 on f.eps_kravhode_id = f2.kravhode_id
)

select
    sak_id,
    vedtak_id,
    kravhode_id,
    ufore_historik_id,
    pen_under_utbet_id,
    beregning_res_id,
    uforetrygd_beregning_id,
    ytelse_komp_id,
    avkort_info_id,
    alder,
    kjonn,
    uforegrad,
    mottar_minsteytelse,
    barnetillegg_flagg,
    antall_barn,
    antall_barn_under_18,
    har_eps,
    eps_kravhode_id,
    yk_brutto,
    yk_netto,
    yk_brutto_per_ar,
    yk_netto_per_ar,
    yk_tfb_brutto,
    yk_tfb_netto,
    yk_tfb_brutto_per_ar,
    yk_tfb_netto_per_ar,
    yk_tsb_brutto,
    yk_tsb_netto,
    yk_tsb_brutto_per_ar,
    yk_tsb_netto_per_ar,
    bt_brutto_ar,
    bt_brutto,
    husholdning_bt_netto,
    husholdning_ny_bt_brutto,
    husholdning_ny_bt_brutto_ar
from final
