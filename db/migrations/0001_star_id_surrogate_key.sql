-- Migration: replace stars.gaia_source_id-as-PK with a surrogate star_id,
-- so a star can be tracked via an alternate catalog (Yale Bright Star
-- Catalogue / BSC5, via bsc_hr_number) when it has no Gaia source_id at all
-- -- a real gap, not a hypothetical: Gaia saturates on the brightest naked
-- -- eye stars. A cross-match of BSC5 against gaiadr3.gaia_source (30"
-- radius, run 2026-07-24) found that of the 170 BSC5 stars brighter than
-- V=3, ~72 (42%) have no credible Gaia counterpart (18 with zero Gaia
-- sources within 30", another 54 where the closest/brightest candidate is
-- >3 mag fainter than expected -- almost certainly an unrelated neighbor,
-- not the star itself, e.g. Arcturus/HR 5340 has zero Gaia sources within
-- 30"). Past V=3 this drops off fast: only 18/1434 (1.3%) of BSC5 stars in
-- 3<=V<5 show the same pattern. So this table only ever expects a small
-- (order ~100) population of source_catalog='bsc5' rows, concentrated
-- among the very brightest stars in the sky.
--
-- This file is written against the live schema as of 2026-07-24 (see
-- db/schema.sql's pre-migration git history for the exact prior shape).
-- Run inside a transaction; review row counts at each step before
-- committing on the production database.

BEGIN;

-- 1. Add the new surrogate PK column and the alternate-catalog columns.
--    Existing rows are untouched here -- star_id is populated in step 2.
ALTER TABLE stars
    ADD COLUMN star_id BIGINT GENERATED ALWAYS AS IDENTITY,
    ADD COLUMN source_catalog TEXT NOT NULL DEFAULT 'gaia',
    ADD COLUMN bsc_hr_number INTEGER;

-- 2. star_id backfills automatically via the IDENTITY default as each
--    existing row is rewritten by the ALTER above (Postgres 10+ populates
--    identity columns for existing rows in the same statement -- verify
--    with `SELECT count(*) FROM stars WHERE star_id IS NULL;` -> 0 before
--    proceeding).

-- 3. spectroscopy_holdings.gaia_source_id_fkey depends on stars_pkey's
--    underlying index (it's the thing being referenced) -- must be dropped
--    before stars_pkey, not after. Confirmed live: doing this in the
--    original order the other way round throws "cannot drop constraint
--    stars_pkey ... because other objects depend on it" and aborts the
--    transaction (harmlessly -- BEGIN/COMMIT means nothing was left
--    half-applied, but it never got past this step on the first attempt).
ALTER TABLE spectroscopy_holdings DROP CONSTRAINT spectroscopy_holdings_gaia_source_id_fkey;

-- 4. Swap primary keys: drop the old gaia_source_id PK, make it a plain
--    UNIQUE + nullable column instead, promote star_id to PK.
ALTER TABLE stars DROP CONSTRAINT stars_pkey;
ALTER TABLE stars ALTER COLUMN gaia_source_id DROP NOT NULL;
ALTER TABLE stars ADD CONSTRAINT stars_pkey PRIMARY KEY (star_id);
ALTER TABLE stars ADD CONSTRAINT stars_gaia_source_id_key UNIQUE (gaia_source_id);
ALTER TABLE stars ADD CONSTRAINT stars_bsc_hr_number_key UNIQUE (bsc_hr_number);

ALTER TABLE stars ADD CONSTRAINT source_catalog_check CHECK (source_catalog IN ('gaia', 'bsc5'));
ALTER TABLE stars ADD CONSTRAINT source_catalog_id_consistency CHECK (
    (source_catalog = 'gaia' AND gaia_source_id IS NOT NULL AND bsc_hr_number IS NULL)
    OR
    (source_catalog = 'bsc5' AND bsc_hr_number IS NOT NULL AND gaia_source_id IS NULL)
);

-- 5. spectroscopy_holdings: add star_id, backfill from the existing
--    gaia_source_id join, then point the FK at it instead.
ALTER TABLE spectroscopy_holdings ADD COLUMN star_id BIGINT;

UPDATE spectroscopy_holdings h
SET star_id = s.star_id
FROM stars s
WHERE h.gaia_source_id = s.gaia_source_id;

-- Sanity check before dropping the old column -- every holding that had a
-- non-null gaia_source_id must now have a non-null star_id:
--   SELECT count(*) FROM spectroscopy_holdings
--   WHERE gaia_source_id IS NOT NULL AND star_id IS NULL;
-- must return 0.

ALTER TABLE spectroscopy_holdings ADD CONSTRAINT spectroscopy_holdings_star_id_fkey
    FOREIGN KEY (star_id) REFERENCES stars(star_id);

-- Dropping gaia_source_id below also drops idx_holdings_gaia_source_id
-- automatically (Postgres cascades that for any index defined solely on a
-- dropped column) -- confirmed live: an explicit DROP INDEX afterward
-- errors with "index does not exist" since it's already gone by then.
ALTER TABLE spectroscopy_holdings DROP COLUMN gaia_source_id;

CREATE INDEX idx_holdings_star_id ON spectroscopy_holdings (star_id);

COMMIT;

-- Application code (sync/matcher.py, ingest/add_star.py, webapp/app.py,
-- scripts/reconcile_name_matches.py, scripts/export_to_parquet.py,
-- tests/test_matcher.py) already updated to match this schema in the same
-- PR that added this migration -- deploy that code right after this
-- migration lands, not before (it assumes star_id exists) and not long
-- after (old code assumes gaia_source_id is still on spectroscopy_holdings).
-- Still not built: an actual BSC5 ingest path (analogous to
-- add_star_by_name, but resolving via HR number / VizieR V/50 instead of
-- the Gaia archive) -- this migration only makes room in the schema for it.
