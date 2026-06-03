{{
  config(
    materialized = 'table',
    tags = ['analyse']
    )
}}

with

finne_eps_til_lovendring as (
    select
        v.sak_id,
        v.vedtak_id,
        pg.person_id,
        v.oifu_endret_flagg,
        v.minsteytelse_ektefelle_flagg,
        v.kompgrad_over_70_flagg,
        case when (
            v.er_minimums_ifu = '1'
            and v.oifu in (429527, 429528, 429529)
            and v.grad != 100
            and v.k_minsteytelseniva in ('ORDINER_EKTEFELLE', 'ORDINER_EKTEF_KONV')
            and v.ny_avrundet_grad - v.grad != 0
        ) or (
            v.oifu > 429529
            and v.oifu < 455560
            and v.grad != 100
            and v.ny_avrundet_grad - v.grad != 0
        ) then 'Bruker får 5% økt grad' end as gradsendring,
        case when yk.netto < yk.brutto then 'Brukers UT er avkortet' end as ut_avkoret,
        case when pd.k_grnl_rolle_t in ('EKTEF', 'PARTNER', 'SAMBO') then 1 else 0 end as har_eps,
        case when pd.k_grnl_rolle_t in ('EKTEF', 'PARTNER', 'SAMBO') then pg.kravhode_id end as eps_kravhode_id,
        pg.PERSON_GRUNNLAG_ID,
        pd.person_det_id
    from pen_dataprodukt.analyse_ifu_ieu_grad v
    left join pen.t_sak s on v.sak_id = s.sak_id
    left join pen.t_person_grunnlag pg on s.person_id = pg.person_id
    left join pen.t_person_det pd on pg.person_grunnlag_id = pd.person_grunnlag_id
    left join pen.t_beregning_res ber
        on
            v.vedtak_id = ber.vedtak_id
            and ber.dato_virk_tom is null
    left join pen.t_ytelse_komp yk
        on
            ber.pen_under_utbet_id = yk.pen_under_utbet_id
            and yk.k_ytelse_komp_t = 'UT_ORDINER'
            and yk.bruk = '1'
    where
        pd.k_grnl_rolle_t in ('EKTEF', 'PARTNER', 'SAMBO')
        and (
            v.oifu_endret_flagg = 1
            or v.minsteytelse_ektefelle_flagg = 1
            or v.kompgrad_over_70_flagg = 1
        )
        and pd.bruk = '1'
        and pd.dato_tom is null
        and pd.rolle_tom is null
),

eps_vedtak_lopende as (
    select
        finne_eps_til_lovendring.*,
        v.vedtak_id as eps_vedtak_id,
        v.person_id as eps_person_id,
        v.k_sak_t as eps_k_sak_t,
        yk_tfb.netto as tfb_netto,
        yk_tfb.brutto as tfb_brutto,
        case
            when yk_tfb.netto > 0 and yk_tfb.netto = yk_tfb.brutto then 'TFB uavkortet'
            when yk_tfb.netto > 0 and yk_tfb.netto < yk_tfb.brutto then 'TFB delvis avkortet'
            when yk_tfb.netto = 0 then 'TFB fullt avkortet'
            when yk_tfb.netto is null then 'ingen TFB'
        end as bt_fb
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
        
),

eps_pavirket_lovendringer as (
    select
        e.vedtak_id,
        e.oifu_endret_flagg,
        e.minsteytelse_ektefelle_flagg,
        e.kompgrad_over_70_flagg,
        e.eps_vedtak_id,
        v.oifu_endret_flagg as oifu_endret_flagg_eps,
        v.minsteytelse_ektefelle_flagg as minsteytelse_ektefelle_flagg_eps,
        v.kompgrad_over_70_flagg as kompgrad_over_70_flagg_eps,
        
        e.bt_fb,
        e.har_eps

    from eps_vedtak_lopende e
    left join pen_dataprodukt.analyse_ifu_ieu_grad v on e.eps_vedtak_id = v.vedtak_id
)

select 
    oifu_endret_flagg,
    minsteytelse_ektefelle_flagg,
    kompgrad_over_70_flagg,
    oifu_endret_flagg_eps,
    minsteytelse_ektefelle_flagg_eps,
    kompgrad_over_70_flagg_eps,
    bt_fb,
    har_eps,
    count(*) as antall

from eps_pavirket_lovendringer
group by
    oifu_endret_flagg,
    minsteytelse_ektefelle_flagg,
    kompgrad_over_70_flagg,
    oifu_endret_flagg_eps,
    minsteytelse_ektefelle_flagg_eps,
    kompgrad_over_70_flagg_eps,
    bt_fb,
    har_eps
order by 
    oifu_endret_flagg,
    minsteytelse_ektefelle_flagg,
    kompgrad_over_70_flagg,
    oifu_endret_flagg_eps,
    minsteytelse_ektefelle_flagg_eps,
    kompgrad_over_70_flagg_eps,        
    bt_fb,
    har_eps