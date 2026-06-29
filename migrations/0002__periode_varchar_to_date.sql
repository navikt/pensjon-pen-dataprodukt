-- Migration: change periode from VARCHAR2 to DATE in stonadsstatistikk marts tables
-- Reason: dbt models now emit to_date(periode, 'YYYYMM'), replacing the raw YYYYMM string
--
-- Run once per table. Safe to re-run: step 1 is idempotent if periode_new already exists.

-- stonadsstatistikk_alder_belop
ALTER TABLE stonadsstatistikk_alder_belop ADD (periode_new DATE);
UPDATE stonadsstatistikk_alder_belop SET periode_new = TO_DATE(periode, 'YYYYMM');
COMMIT;
ALTER TABLE stonadsstatistikk_alder_belop DROP COLUMN periode;
ALTER TABLE stonadsstatistikk_alder_belop RENAME COLUMN periode_new TO periode;

-- stonadsstatistikk_alder_beregning
ALTER TABLE stonadsstatistikk_alder_beregning ADD (periode_new DATE);
UPDATE stonadsstatistikk_alder_beregning SET periode_new = TO_DATE(periode, 'YYYYMM');
COMMIT;
ALTER TABLE stonadsstatistikk_alder_beregning DROP COLUMN periode;
ALTER TABLE stonadsstatistikk_alder_beregning RENAME COLUMN periode_new TO periode;

-- stonadsstatistikk_alder_vedtak
ALTER TABLE stonadsstatistikk_alder_vedtak ADD (periode_new DATE);
UPDATE stonadsstatistikk_alder_vedtak SET periode_new = TO_DATE(periode, 'YYYYMM');
COMMIT;
ALTER TABLE stonadsstatistikk_alder_vedtak DROP COLUMN periode;
ALTER TABLE stonadsstatistikk_alder_vedtak RENAME COLUMN periode_new TO periode;
