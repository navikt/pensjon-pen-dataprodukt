-- int_flere_joins_pilot
-- joiner inn siste tabeller fra sql-pilot og setter kolonner som skal være med i final

with

ref_int_aktive_alder as (
    select *
    from {{ ref('int_aktive_alder') }}
),

ref_stg_k_afp_t as (
    select
        k_afp_t,
        dekode
    from {{ ref('stg_t_k_afp_t') }}
),

ref_stg_k_regelverk_t as (
    select
        k_regelverk_t,
        dekode
    from {{ ref('stg_t_k_regelverk_t') }}
),

ref_stg_beregning_info as (
    select
        beregning_info_id,
        mottar_min_pensjonsniva,
        gjenlevrett_anv,
        inst_opph_anv,
        yrksk_grad,
        yrksk_anv,
        tt_anv
    from {{ ref('stg_t_beregning_info') }}
),

-- ref_stg_beregning_info_2025 as (
--     -- egen join på beregning_info_id_2025, som ble laget i int_aktive_alder_kap19
--     select
--         beregning_info_id,
--         tt_anv
--     from {{ ref('stg_t_beregning_info') }}
-- ),

ref_uttaksgrad as (
    -- brukes her til å sette afp_privat_flagg
    select
        sak_id,
        uttaksgrad,
        dato_virk_fom,
        dato_virk_tom
    from {{ ref('stg_t_uttaksgrad') }}
),

ref_vedtak as (
    -- brukes her til å sette afp_privat_flagg
    select
        vedtak_id,
        sak_id,
        k_sak_t,
        dato_lopende_fom,
        dato_lopende_tom
    from {{ ref('stg_t_vedtak') }}
),

sette_afp_privat_flagg as (
    -- jeg får litt vondt av å se på denne
    select
        aktive.*,
        case
            when exists (
                select null
                from ref_vedtak
                where
                    ref_vedtak.vedtak_id = aktive.vedtak_id -- endret fra person_id
                    and ref_vedtak.dato_lopende_fom <= to_date({{ var("periode") }}, 'YYYYMMDD')
                    and (ref_vedtak.dato_lopende_tom is null or ref_vedtak.dato_lopende_tom >= to_date({{ var("periode") }}, 'YYYYMMDD'))
                    and ref_vedtak.k_sak_t = 'AFP_PRIVAT'
                    and not exists (
                        select null
                        from ref_uttaksgrad
                        where
                            ref_uttaksgrad.sak_id = ref_vedtak.sak_id
                            and ref_uttaksgrad.dato_virk_fom <= to_date({{ var("periode") }}, 'YYYYMMDD')
                            and (ref_uttaksgrad.dato_virk_tom is null or ref_uttaksgrad.dato_virk_tom >= to_date({{ var("periode") }}, 'YYYYMMDD'))
                            and ref_uttaksgrad.uttaksgrad = 0
                    )
            ) then 1
            else 0
        end as afp_privat_flagg
    from ref_int_aktive_alder aktive
),

hente_beregning_info as (
    select
        aktive.*,
        ref_stg_beregning_info.gjenlevrett_anv,
        to_number(coalesce(aktive.red_pga_inst_opph_flagg, ref_stg_beregning_info.inst_opph_anv)) as red_pga_inst_opph_flagg_final,
        -- coalesce(aktive.tt_anvendt_kap19_antall, ref_stg_beregning_info.tt_anv) as tt_anvendt_kap19_antall,
        to_number( -- i sql-pilot er to_number her spesifisert med format '9' (0-9)
            coalesce(
                aktive.minstepensjonist,
                ref_stg_beregning_info.mottar_min_pensjonsniva
            )
        ) as minstepensjon,
        coalesce(
            aktive.anvendt_yrkesskade_flagg,
            case
                when coalesce(ref_stg_beregning_info.yrksk_anv, '0') = '1' and ref_stg_beregning_info.yrksk_grad > 0 then 1
                else 0
            end
        ) as anvendt_yrkesskade_flagg_final
        -- coalesce(
        --     case
        --         when aktive.regelverk = 'N_REG_G_N_OPPTJ'
        --             then ref_stg_beregning_info_2025.tt_anv
        --     end,
        --     aktive.tt_anvendt_kap20_antall
        -- ) as tt_anvendt_kap20_antall
    from sette_afp_privat_flagg aktive
    left outer join ref_stg_beregning_info
        on ref_stg_beregning_info.beregning_info_id = case
            when aktive.regelverk = 'N_REG_G_N_OPPTJ' then aktive.beregning_info_id_2016
            else aktive.beregning_info_id
        end
-- left outer join ref_stg_beregning_info_2025
--     on aktive.beregning_info_id_2025 = ref_stg_beregning_info_2025.beregning_info_id
),

kobler_kodeverk as (
    select
        aktive.*,
        ref_stg_k_afp_t.dekode as afp_ordning,
        coalesce(ref_stg_k_regelverk_t.dekode, 'AP kap 19 tom 2010') as regelverk_final,
        case
            when aktive.k_sak_t = 'ALDER' and aktive.netto > 0 then 1
            else 0
        end as aldersytelseflagg
    from hente_beregning_info aktive
    left join ref_stg_k_afp_t
        on aktive.k_afp_t = ref_stg_k_afp_t.k_afp_t
    left join ref_stg_k_regelverk_t
        on aktive.regelverk = ref_stg_k_regelverk_t.k_regelverk_t
),

final as (
    select
        sak_id,
        vedtak_id,
        kravhode_id,
        grein,
        k_sak_t as sakstype,
        -- k_afp_t, -- todo: skal ikke være med
        -- dato_virk_tom, -- todo: skal ikke være med
        -- dato_virk_fom, -- todo: skal ikke være med
        -- beregning_id, -- todo: skal ikke være med
        -- beregning_info_id, -- todo: skal ikke være med
        -- beregning_info_id_2016, -- todo: skal ikke være med
        -- beregning_info_id_2025, -- todo: skal ikke være med
        -- beregning_info_id_avdod, -- todo: skal ikke være med
        -- beregning_info_id_avdod_2016, -- todo: skal ikke være med
        -- pen_under_utbet_id, -- todo: skal ikke være med
        uttaksgrad,
        -- brutto, -- todo: skal ikke være med
        netto,
        gp_netto,
        tp_netto,
        pt_netto,
        st_netto,
        et_netto,
        ip_netto,
        gap_netto,
        gjt_netto,
        gjt_k19_netto,
        skjermt_netto,
        saerkull_netto,
        mpn_sstot_netto,
        mpn_indiv_netto,
        barn_felles_netto,
        ufor_sum_ut_ord_netto,
        ap_kap19_uten_gjr_bel,
        ap_kap19_med_gjr_bel,
        -- minstepensjonist, -- todo: skal ikke være med
        minste_pen_niva,
        afp_lopph_flagg,
        afp_finans_flagg,
        afp_lopph_ytelse_flagg,
        -- todo overgangsstonad,
        -- nye kolonner i denne modellen
        afp_privat_flagg,
        gjenlevrett_anv,
        red_pga_inst_opph_flagg_final as red_pga_inst_opph_flagg,
        0 as tt_anvendt_kap19_antall, -- må skjønne hva disse skal være
        0 as tt_anvendt_kap20_antall, -- må skjønne hva disse skal være
        minstepensjon,
        anvendt_yrkesskade_flagg_final as anvendt_yrkesskade_flagg,
        aldersytelseflagg,
        afp_ordning,
        regelverk_final as regelverk,
        sysdate as kjoretidspunkt
        -- cast('00000000000' as varchar2(11)) as persnr
    from kobler_kodeverk
)

select * from final
