with

gruppe_a as (
    select
        count(*) as ant,
        'IFU 3,3G og 100% grad' as kommentar
    from pen_dataprodukt.analyse_ifu_ieu_grad
    where
        1 = 1
        and er_minimums_ifu = '1'
        and oifu in (429527, 429528, 429529)
        and grad = 100
        and k_minsteytelseniva in ('ORDINER_EKTEFELLE', 'ORDINER_EKTEF_KONV')
),

gruppe_b as (
    select
        count(*) as ant,
        'IFU 3,3G gradert uten endring i grad' as kommentar
    from pen_dataprodukt.analyse_ifu_ieu_grad
    where
        1 = 1
        and er_minimums_ifu = '1'
        and oifu in (429527, 429528, 429529)
        and grad != 100
        and k_minsteytelseniva in ('ORDINER_EKTEFELLE', 'ORDINER_EKTEF_KONV')
        --and k_minimum_ifu_t = 'MINIMUM_IFU_GIFT'
        and ny_avrundet_grad - grad = 0
),

gruppe_c as (
    select
        count(*) as ant,
        'IFU 3,3G gradert med 5% endring i grad' as kommentar
    from pen_dataprodukt.analyse_ifu_ieu_grad
    where
        1 = 1
        and er_minimums_ifu = '1'
        and oifu in (429527, 429528, 429529)
        and grad != 100
        and k_minsteytelseniva in ('ORDINER_EKTEFELLE', 'ORDINER_EKTEF_KONV')
        --and k_minimum_ifu_t = 'MINIMUM_IFU_GIFT'
        and ny_avrundet_grad - grad != 0
),

gruppe_a1 as (
    select
        count(*) as ant,
        'OIFU mellom 3,3G og 3,5G og 100% grad' as kommentar
    from pen_dataprodukt.analyse_ifu_ieu_grad
    where
        1 = 1
        and oifu > 429529
        and oifu < 455560
        and grad = 100
),

gruppe_b1 as (
    select
        count(*) as ant,
        'OIFU mellom 3,3G og 3,5G og gradert' as kommentar
    from pen_dataprodukt.analyse_ifu_ieu_grad
    where
        1 = 1
        and oifu > 429529
        and oifu < 455560
        and grad != 100
        and ny_avrundet_grad - grad = 0
),

gruppe_c1 as (
    select
        count(*) as ant,
        'OIFU mellom 3,3G og 3,5G og gradert og 5% endring i grad' as kommentar
    from pen_dataprodukt.analyse_ifu_ieu_grad
    where
        1 = 1
        and oifu > 429529
        and oifu < 455560
        and grad != 100
        and ny_avrundet_grad - grad != 0
),

alle as (
    select * from gruppe_a
    union all
    select * from gruppe_b
    union all
    select * from gruppe_c
    union all
    select * from gruppe_a1
    union all
    select * from gruppe_b1
    union all
    select * from gruppe_c1
)

select * from alle
