-- Migration: add two outcomes to the crowdsourced-triage design --
-- 'attach_bright_star' (a real star too bright for Gaia to have detected at
-- all, tracked via `stars.bsc_hr_number` since 0001_star_id_surrogate_key.sql
-- -- that migration made room for it but nothing downstream of /triage could
-- record a vote for it yet) and 'not_a_star' (a real astronomical object,
-- just not a stellar one -- previously lumped into 'not_a_real_target',
-- which conflated genuine non-targets like calibration frames with genuine
-- non-stellar targets like galaxies).
--
-- Run inside a transaction against the live database.

BEGIN;

ALTER TABLE skip_classifications DROP CONSTRAINT skip_classifications_outcome_check;
ALTER TABLE skip_classifications ADD CONSTRAINT skip_classifications_outcome_check CHECK (outcome IN (
    'attach_gaia_source',
    'attach_bright_star',
    'not_a_real_target',
    'not_a_star',
    'confirmed_absent_from_gaia'
));

ALTER TABLE skip_classifications ADD COLUMN proposed_bsc_hr_number INTEGER;
ALTER TABLE skip_classifications ADD CONSTRAINT skip_classifications_bsc_hr_number_check CHECK (
    (outcome = 'attach_bright_star') = (proposed_bsc_hr_number IS NOT NULL)
);

COMMIT;

-- Application code (webapp/app.py, scripts/export_to_parquet.py,
-- scripts/joy_triage_append.py) already updated to match this schema in the
-- same PR that added this migration -- deploy that code alongside this
-- migration, not before (old code doesn't know the new outcomes exist, so
-- it's harmless either order) and not long after (new code will submit
-- 'attach_bright_star'/'not_a_star' outcomes and proposed_bsc_hr_number
-- values this migration hasn't made room for yet).
