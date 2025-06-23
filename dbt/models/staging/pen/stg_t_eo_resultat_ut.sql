-- stg_t_eo_resultat_ut

select
    eo_resultat_ut_id,
    avviksbelop,
    avviksbelop_ut,
    avviksbelop_tfb,
    avviksbelop_tsb,
    inntekt_ut,
    inntekt_tfb,
    inntekt_tsb,
    k_ut_eo_resultat,
    dato_endret
    -- dato_opprettet,
    -- opprettet_av,
    -- endret_av,
    -- rettsgebyr,
    -- toleransegrense_negativ,
    -- toleransegrense_positiv,
    -- total_belop,
    -- total_belop_ut,
    -- total_belop_tsb,
    -- total_belop_tfb,
    -- tidligere_belop,
    -- tidligere_belop_ut,
    -- tidligere_belop_tsb,
    -- tidligere_belop_tfb,
    -- versjon,
from {{ source('pen', 't_eo_resultat_ut') }}
