with

ref_int_lopende_vedtak_alder as (
    select
        vedtak_id,
        sak_id,
        kravhode_id,
        k_sak_t,
        dato_lopende_fom,
        dato_lopende_tom,
        k_regelverk_t
    from {{ ref('int_lopende_vedtak_alder') }}
),

ref_t_vilkar_vedtak as (
    select
        vedtak_id,
        k_vilk_vurd_t,
        dato_virk_fom,
        dato_virk_tom
    from {{ ref('stg_t_vilkar_vedtak') }}
),

overgangsstonad as (
    select
        id,
        rtrim(ltrim(max(case when id2 = 1 then splitted_element end))) as kode,
        max(case when id2 = 5 then splitted_element end) as flagg
    from (
        select
            id,
            level as id2,
            regexp_substr(tekst, '[^;]+', 1, level) as splitted_element
        from (
            select
                1 as id,
                ';INNV1;189;Innvilget § 17-10.1a;0;1;1;   ;;;;;;;;' as tekst
            from dual
            union
            select
                2 as id,
                ';INNV2;189;Innvilget § 17-10.1b;0;1;1;   ;;;;;;;;' as tekst
            from dual
            union
            select
                3 as id,
                ';INNV3;189;Innvilget § 17-10.1c;0;1;1;   ;;;;;;;;' as tekst
            from dual
            union
            select
                4 as id,
                ';INNV4;189;Innvilget § 17-10.2;0;1;1;   ;;;;;;;;' as tekst
            from dual
            union
            select
                5 as id,
                ';INNV5;189;Innvilget gammel § 10-6 (før 1989);0;1;1;   ;;;;;;;;' as tekst
            from dual
            union
            select
                6 as id,
                ';EGNE_BARN_GJP;189;Egne barn - Overgangsstønad;1;0;1;   ;;;;;;;;' as tekst
            from dual
            union
            select
                7 as id,
                ';EGNE_BARN_KUN_GJP;189;Egne barn;0;1;1;   ;;;;;;;;' as tekst
            from dual
            union
            select
                8 as id,
                ';UTDAN_GJP;189;Nødvendig utdanning - Overgangsstønad;1;0;1;   ;;;;;;;;' as tekst
            from dual
            union
            select
                9 as id,
                ';;OMS_AVD_BARN_GJP;189;Omsorg avdødes barn - Overgangsstønad;1;0;1;   ;;;;;;;;' as tekst
            from dual
            union
            select
                10 as id,
                ';OMS_AVD_BARN_KUN_GJP;189;Omsorg avdødes barn;0;1;1;   ;;;;;;;;' as tekst
            from dual
            union
            select
                11 as id,
                ';OMSTILL_GJP;189;Omstillingsperiode - Overgangsstønad;1;0;1;   ;;;;;;;;' as tekst
            from dual
            union
            select
                12 as id,
                ';EGNE_BARN_FP;188;Egne barn - Overgangsstønad;1;0;1;   ;;;;;;;;' as tekst
            from dual
            union
            select
                13 as id,
                ';EGNE_BARN_KUN_FP;188;Egne barn;0;1;1;   ;;;;;;;;' as tekst
            from dual
            union
            select
                14 as id,
                ';UTDAN_FP;188;Nødvendig utdanning - Overgangsstønad;1;0;1;   ;;;;;;;;' as tekst
            from dual
            union
            select
                15 as id,
                ';OMS_AVD_BARN_FP;188;Omsorg avdødes barn - Overgangsstønad;1;0;1;' as tekst
            from dual
            union
            select
                16 as id,
                ';OMS_AVD_BARN_KUN_FP;188;Omsorg avdødes barn;0;1;1;' as tekst
            from dual
            union
            select
                17 as id,
                ';OMSTILL_FP;188;Omstillingsperiode - Overgangsstønad;1;0;1;' as tekst
            from dual
            union
            select
                18 as id,
                ';UNDER_UTDAN;185;Under utdanning;1;0;1;' as tekst
            from dual
            union
            select
                19 as id,
                ';PRAKTIK;185;Lærling/praktikant;1;0;1;' as tekst
            from dual
            union
            select
                20 as id,
                ';INNV1_GJR;194;Innvilget §17-10.1a;0;1;1;' as tekst
            from dual
            union
            select
                21 as id,
                ';INNV2_GJR;194;Innvilget §17-10.1b;0;1;1;' as tekst
            from dual
            union
            select
                22 as id,
                ';INNV3_GJR;194;Innvilget § 17-10.1c;0;1;1;' as tekst
            from dual
            union
            select
                23 as id,
                ';INNV4_GJR;194;Innvilget § 17- 10.2;0;1;1;' as tekst
            from dual
            union
            select
                24 as id,
                ';INNV5_GJR;194;Innvilget gammel §10-16 (før 1989);0;1;1;' as tekst
            from dual
            union
            select
                25 as id,
                ';EGNE_BARN_GJR;194;Egne barn - Overgangsstønad;1;0;1;' as tekst
            from dual
            union
            select
                26 as id,
                ';EGNE_BARN_KUN_GJR;194;Egne barn;0;1;1;' as tekst
            from dual
            union
            select
                27 as id,
                ';UTDAN_GJR;194;Nødvendig utdanning - Overgangsstønad;1;0;1;' as tekst
            from dual
            union
            select
                28 as id,
                ';OMS_AVD_BARN_GJR;194;Omsorg avdødes barn - Overgangsstønad;1;0;1;' as tekst
            from dual
            union
            select
                29 as id,
                ';OMS_AVD_BARN_KUN_GJR;194;Omsorg avdødes barn;0;1;1;' as tekst
            from dual
            union
            select
                30 as id,
                ';OMSTILL_GJR;194;Omstillingsperiode - Overgangsstønad;1;0;1;' as tekst
            from dual
        --                 union select   31  as id, '; ;;;;;;;;;;;;;;' from dual
        )

        connect by instr(tekst, ';', 1, level) > 0
        and id = prior id
        and prior dbms_random.value is not null
    )
    where id2 in (1, 5)
    group by id
),

tvvx as (
    select
        x.vedtak_id,
        rtrim(x.k_vilk_vurd_t) as k_vilk_vurd_t
    from (
        select
            ref_int_lopende_vedtak_alder.vedtak_id,
            ref_t_vilkar_vedtak.k_vilk_vurd_t,
            row_number() over (partition by ref_int_lopende_vedtak_alder.vedtak_id order by ref_int_lopende_vedtak_alder.vedtak_id) as rn
        from ref_int_lopende_vedtak_alder
        left outer join ref_t_vilkar_vedtak on ref_int_lopende_vedtak_alder.vedtak_id = ref_t_vilkar_vedtak.vedtak_id
        where
            ref_t_vilkar_vedtak.dato_virk_fom <= {{ periode_sluttdato(var("periode")) }}
            and (ref_t_vilkar_vedtak.dato_virk_tom >= trunc({{ periode_sluttdato(var("periode")) }}) or ref_t_vilkar_vedtak.dato_virk_tom is null)
    ) x
    where x.rn = 1
),

join_ovs as (
    select
        aktive.vedtak_id,
        ost.flagg
    from ref_int_lopende_vedtak_alder aktive
    left outer join tvvx on aktive.vedtak_id = tvvx.vedtak_id
    left outer join overgangsstonad ost on tvvx.k_vilk_vurd_t = ost.kode

)

select * from join_ovs
