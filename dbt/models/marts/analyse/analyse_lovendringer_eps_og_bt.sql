-- analyse_lovendringer_eps_og_bt

with

finne_eps_til_lovendring as (
    select
        v.sak_id,
        v.vedtak_id,
        v.oifu_endret_flagg,
        pg.person_id,
        v.minsteytelse_ektefelle_flagg,
        case when (
            v.er_minimums_ifu = '1'
            and v.oifu in (429527, 429528, 429529)
            and v.grad != 100
            and v.k_minsteytelseniva in ('ORDINER_EKTEFELLE', 'ORDINER_EKTEF_KONV')
            --and k_minimum_ifu_t = 'MINIMUM_IFU_GIFT'
            and v.ny_avrundet_grad - v.grad != 0
        ) or (
            v.oifu > 429529
            and v.oifu < 455560
            and v.grad != 100
            and v.ny_avrundet_grad - v.grad != 0
        ) then '5%' else '-' end as gradsendring,
        v.kompgrad_over_70_flagg,
        pd.k_grnl_rolle_t,
        pg.kravhode_id as eps_kravhode_id
    from pen_dataprodukt.analyse_ifu_ieu_grad v
    left join pen.t_sak s on v.sak_id = s.sak_id
    left join pen.t_person_grunnlag pg on s.person_id = pg.person_id
    left join pen.t_person_det pd on pg.person_grunnlag_id = pd.person_grunnlag_id
    where
        pd.k_grnl_rolle_t in ('EKTEF', 'PARTNER', 'SAMBO')
        and (
            v.oifu_endret_flagg = 1
            or v.minsteytelse_ektefelle_flagg = 1
            or v.kompgrad_over_70_flagg = 1
        )
        and pd.bruk = '1'
        and pd.dato_tom is null
    order by v.sak_id desc
),

eps_vedtak_lopende as (
    select
        finne_eps_til_lovendring.*,
        v.vedtak_id as eps_vedtak_id,
        v.person_id as eps_person_id,
        v.k_sak_t as eps_k_sak_t,
        yk_tfb.netto as tfb_netto,
        yk_tfb.brutto as tfb_brutto
    from finne_eps_til_lovendring
    left join pen.t_vedtak v on finne_eps_til_lovendring.eps_kravhode_id = v.kravhode_id
    left join pen.t_beregning_res ber
        on
            v.vedtak_id = ber.vedtak_id
            and ber.dato_virk_tom is null
    left join pen.t_ytelse_komp yk_tfb
        on
            ber.pen_under_utbet_id = yk_tfb.pen_under_utbet_id
            and yk_tfb.k_ytelse_komp_t = 'UT_TFB'
            and yk_tfb.bruk = '1'
            and yk_tfb.opphort = '0'
    where
        v.dato_lopende_fom is not null
        and v.dato_lopende_tom is null
        and v.k_sak_t = 'UFOREP'
)

select
    count(*) as antall,
    oifu_endret_flagg as okt_ifu,
    minsteytelse_ektefelle_flagg as ny_minsteytelse_ekft,
    kompgrad_over_70_flagg as kompgrad_over_70,
    case
        when tfb_netto > 0 and tfb_netto = tfb_brutto then 'TFB uavkortet'
        when tfb_netto > 0 and tfb_netto < tfb_brutto then 'TFB delvis avkortet'
        when tfb_netto = 0 then 'TFB fullt avkortet'
        when tfb_netto is null then 'ingen TFB'
        else 'obs!'
    end as bt_fb
from eps_vedtak_lopende
group by
    oifu_endret_flagg,
    minsteytelse_ektefelle_flagg,
    kompgrad_over_70_flagg,
    case
        when tfb_netto > 0 and tfb_netto = tfb_brutto then 'TFB uavkortet'
        when tfb_netto > 0 and tfb_netto < tfb_brutto then 'TFB delvis avkortet'
        when tfb_netto = 0 then 'TFB fullt avkortet'
        when tfb_netto is null then 'ingen TFB'
        else 'obs!'
    end
order by bt_fb desc, antall desc
