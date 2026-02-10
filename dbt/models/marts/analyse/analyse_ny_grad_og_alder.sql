-- analyse_ny_grad_og_alder
-- finner de som får 5% økt grad OG har en alderssak
-- er en case med de som har sum av grader over 100%
with base as (
    select
        s_u.k_sak_s as status_u,
        s_a.k_sak_s as status_a,
        an.grad + ug.uttaksgrad as gammel_grad_sum,
        an.ny_avrundet_grad + ug.uttaksgrad as ny_grad_sum,
        an.grad,
        an.ny_avrundet_grad,
        s_a.k_sak_t as sak_alder,
        coalesce(ug.uttaksgrad, -1) as uttaksgrad_a,
        s_u.k_sak_t as sak_type_u,
        s_a.k_sak_t as sak_type_a,
        an.sak_id as sak_id_u,
        s_a.sak_id as sak_id_a,
        s_u.person_id
    from pen_dataprodukt.analyse_ifu_ieu_grad an
    left join pen.t_sak s_u on an.sak_id = s_u.sak_id
    inner join pen.t_sak s_a on s_u.person_id = s_a.person_id and s_a.k_sak_t = 'ALDER'
    left join pen.t_uttaksgrad ug on s_a.sak_id = ug.sak_id
    where
        ( -- 'IFU 3,3G gradert med 5% endring i grad' as kommentar
            an.er_minimums_ifu = '1'
            and an.oifu in (429527, 429528, 429529)
            and an.grad != 100
            and an.k_minsteytelseniva in ('ORDINER_EKTEFELLE', 'ORDINER_EKTEF_KONV')
            and an.ny_avrundet_grad - an.grad != 0
        )
        or
        ( -- 'OIFU mellom 3,3G og 3,5G og gradert og 5% endring i grad' as kommentar
            an.oifu > 429529
            and an.oifu < 455560
            and an.grad != 100
            and an.ny_avrundet_grad - an.grad != 0
        )
)

select
    count(*) as antall,
    status_u,
    status_a,
    gammel_grad_sum,
    ny_grad_sum
from base
group by status_u, status_a, gammel_grad_sum, ny_grad_sum
order by ny_grad_sum desc, status_a asc
