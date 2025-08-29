-- alderspensjon_vedtak
-- placeholder for hva vi skal ende opp med

select
    1 as periode,
    1 as person_id,
    1 as sak_id,
    1 as kravhode_id,
    1 as vedtak_id,
    'a' as k_sak_t,
    'a' as k_vedtak_t,
    'a' as k_regelverk_t,
    'a' as k_minstepen_nivaa,
    'a' as grad,
    1 as pro_rata_teller,
    1 as pro_rata_nevner,
    'a' as yrkesskade_anvendt, -- gravejobb
    'a' as institusjonsopphold_info, -- gravejobb
    1 as tt_anv_kap19,
    1 as tt_anv_kap20,

    1 as pensjonsbeholdning,
    1 as pensjonsbeholdning_garanti,
    1 as pensjonsbeholdning_garanti_tillegg,

    1 as pensjonsgivende_inntekt,
    1 as bostatus_grunnlag,
    1 as bostatus_bor_med,
    'a' as gradendringstatus,

    '2024-01-01'::date as dato_lopende_fom,
    '2024-01-01'::date as dato_lopende_tom,
    '2024-01-01'::date as dato_virk_fom,
    '2024-01-01'::date as dato_virk_tom,
    '2024-01-01'::date as dato_vedtak
from dual
