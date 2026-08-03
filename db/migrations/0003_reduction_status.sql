-- Migration: track per-record reduction status (raw vs. pipeline-reduced)
-- on spectroscopy_holdings.
--
-- Deliberately a coarse 2-way bucket ('raw' / 'reduced' / 'unknown'), not
-- the full IVOA ObsCore calib_level scale (0=raw telemetry, 1=instrument-
-- signature-removed, 2=calibrated to standard units, 3=enhanced/combined)
-- some archives derive it from -- see sync.base.reduction_status_from_
-- calib_level. 'unknown' is the honest default: most archives here have no
-- calib_level column or other documented processing-stage signal at all
-- (a plain HTML-form/bulk-file/SSA archive rarely says which stage its one
-- download link serves).
--
-- Populated by application code (same PR as this migration) for:
--   mast, mast_jwst, eso, cfht_cadc, dao, gemini, oirsa
--     -- real ObsCore calib_level column, confirmed live 2026-08-03 to be
--        populated and genuinely varying (e.g. CADC: CFHT=2, DAO=1, Gemini
--        MAROON-X=1; MAST: FUSE=2, JWST=3, HST=2-3; ESO PESSTO=2; OIRSA=1
--        across all four of its instruments).
--   koa -- every table synced here is one of KOA's raw per-instrument
--          tables (koa_reduced_data, the actual processed-products table,
--          is deliberately out of scope -- see koa.py's own docstring).
--   gemini_ghost, gemini_igrins -- both already filter GOA's file listing
--          down to reduced-product filenames before a record is ever built.
--   naoj -- already ranks/picks one specific product tier per exposure;
--          that pick now also drives reduction_status.
-- Every other archive stays 'unknown' until a real per-archive signal is
-- found -- not a guess.
--
-- Run inside a transaction against the live database.

BEGIN;

ALTER TABLE spectroscopy_holdings
    ADD COLUMN reduction_status TEXT NOT NULL DEFAULT 'unknown'
        CHECK (reduction_status IN ('raw', 'reduced', 'unknown'));

COMMIT;

-- Application code (sync/base.py, sync/matcher.py, and the archive modules
-- listed above) already updated to match this schema in the same PR that
-- added this migration -- deploy alongside it. Existing rows all default
-- to 'unknown' and stay that way: sync jobs are incremental (each archive's
-- cursor only fetches records newer than its watermark), so a normal
-- re-sync will NOT touch already-synced rows -- only newly-ingested
-- records for the archives listed above will carry a real reduction_status
-- via sync.matcher's ON CONFLICT upsert. Backfilling existing rows for
-- those archives would need a one-off script that re-fetches and re-
-- upserts from cursor 0 -- not built here, left as a TODO if backfilling
-- existing rows turns out to matter.
