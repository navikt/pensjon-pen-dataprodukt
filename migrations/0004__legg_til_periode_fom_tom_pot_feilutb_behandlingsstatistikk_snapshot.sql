-- legg til periode_fom, periode_tom og pot__tilbakek i snapshot-tabeller for behandlingsstatistikk
-- gjelder kun rader med k_krav_gjelder = 'TILBAKEKR'
-- verdiene aggregeres fra pen.t_tilbakek_pr_mnd via kravhode_id -> t_vedtak -> t_tilbakek_total -> t_tilbakek_pr_ar -> t_tilbakek_pr_mnd

-- snapshot_int_ufore_behandling_grunnlag
alter table snapshot_int_ufore_behandling_grunnlag add (periode_fom date);
alter table snapshot_int_ufore_behandling_grunnlag add (periode_tom date);
alter table snapshot_int_ufore_behandling_grunnlag add (pot__tilbakek number);

update snapshot_int_ufore_behandling_grunnlag snap
set (snap.periode_fom, snap.periode_tom, snap.pot__tilbakek) = (
    select
        min(m.periode_fom),
        max(m.periode_tom),
        sum(to_number(m.pot__tilbakek))
    from pen.t_tilbakek_total tt
    inner join pen.t_tilbakek_pr_ar a on a.tilbakek_total_id = tt.tilbakek_total_id
    inner join pen.t_tilbakek_pr_mnd m on m.tilbakek_pr_ar_id = a.tilbakek_pr_ar_id
    inner join pen.t_vedtak v on v.vedtak_id = tt.vedtak_id
    where v.kravhode_id = snap.kravhode_id
      and m.pot__tilbakek > 0
)
where snap.k_krav_gjelder = 'TILBAKEKR';

commit;


-- snapshot_int_alder_behandling_grunnlag
alter table snapshot_int_alder_behandling_grunnlag add (periode_fom date);
alter table snapshot_int_alder_behandling_grunnlag add (periode_tom date);
alter table snapshot_int_alder_behandling_grunnlag add (pot__tilbakek number);

update snapshot_int_alder_behandling_grunnlag snap
set (snap.periode_fom, snap.periode_tom, snap.pot__tilbakek) = (
    select
        min(m.periode_fom),
        max(m.periode_tom),
        sum(to_number(m.pot__tilbakek))
    from pen.t_tilbakek_total tt
    inner join pen.t_tilbakek_pr_ar a on a.tilbakek_total_id = tt.tilbakek_total_id
    inner join pen.t_tilbakek_pr_mnd m on m.tilbakek_pr_ar_id = a.tilbakek_pr_ar_id
    inner join pen.t_vedtak v on v.vedtak_id = tt.vedtak_id
    where v.kravhode_id = snap.kravhode_id
      and m.pot__tilbakek > 0
)
where snap.k_krav_gjelder = 'TILBAKEKR';

commit;
