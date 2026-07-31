-- Migration: SDSS DR20 shipped 2026-07-31 (cumulative over DR19). Live-
-- verified against the real DR20 spAll FITS header that sdss_v_optical's
-- GAIA_ID column switched from Gaia DR2 to DR3 source_id, as the row's own
-- prior note anticipated. archives.native_gaia_dr is descriptive metadata
-- only (not read by any application code) -- this just keeps it truthful.
-- sync/archives/sdss_v_optical.py, sdss_v_apogee.py, and
-- sdss_legacy_optical.py were also repointed at DR20 URLs in the same PR.
--
-- Run inside a transaction against the live database.

BEGIN;

UPDATE archives
SET native_gaia_dr = 'dr3',
    notes = 'Implemented directly against the bulk spAll-lite file (now DR20, ~2.5GB gzip) — GAIA_ID 100% populated for CLASS=STAR in the DR19 sample, including live-confirmed FPS-era rows. DR20 (shipped 2026-07-31) confirmed live via the FITS header that GAIA_ID switched from Gaia DR2 to DR3 source_id, as expected.'
WHERE archive_code = 'sdss_v_optical';

COMMIT;
