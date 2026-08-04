-- Migration: gaia_rvs moves from an after_star_id watermark over this
-- project's own `stars` table (0004) to a real independent-discovery
-- archive -- sync/archives/gaia_rvs.py now queries gaiadr3.gaia_source
-- directly via Gaia's own TAP for every source with has_rvs='true'
-- (999,645 as of DR3, confirmed live), source_id-watermark paginated, the
-- same shape as any other TAP archive in this project. Previously this
-- project only carried an RVS holding for the 734,664 of those stars also
-- seen by some other archive; the first sync.main run under the new
-- fetch() re-walks the full set from source_id 0 (the old after_star_id
-- cursor key is silently ignored) to pick up the remaining ~265k RVS stars
-- this project had no prior reason to track. This migration only updates
-- archives' descriptive metadata to match -- no data migration needed, the
-- sync run itself backfills spectroscopy_holdings.
--
-- Run inside a transaction against the live database.

BEGIN;

UPDATE archives
SET access_mechanism = 'tap',
    notes = 'sync/archives/gaia_rvs.py queries gaiadr3.gaia_source directly via Gaia''s own TAP service for every source with has_rvs=''true'' (999,645 as of DR3), source_id-watermark paginated — an independent discovery archive like any other, not limited to stars already tracked via some other archive.'
WHERE archive_code = 'gaia_rvs';

COMMIT;
