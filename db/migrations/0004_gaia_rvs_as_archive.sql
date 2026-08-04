-- Migration: gaia_rvs moves from a special-cased INSERT inside
-- ingest.add_star (run as a side effect of registering any star) to a real
-- sync/archives/gaia_rvs.py module, run through the normal sync.runner
-- pipeline like every other archive -- see that module's docstring for why
-- (has_rvs comes back on the same gaia_source row add_star already reads,
-- so there's no external service to poll; the new fetch() instead walks our
-- own tracked `stars` table). No behavior change for already-synced rows --
-- gaia_rvs holdings already in spectroscopy_holdings are untouched, and the
-- new module's star_id-watermark cursor starts at 0, so the first run just
-- re-confirms (ON CONFLICT DO NOTHING via the matcher's upsert) every
-- has_gaia_rvs star already tracked before catching up on any added since.
-- This migration only updates archives' descriptive metadata to match.
--
-- Run inside a transaction against the live database.

BEGIN;

UPDATE archives
SET access_mechanism = 'internal',
    notes = 'Native to Gaia itself — has_gaia_rvs comes back on the same gaia_source row ingest.add_star already reads to register a star, so there''s no external service to query. sync/archives/gaia_rvs.py instead walks our own tracked `stars` table (star_id watermark cursor) and reports a holding for each has_gaia_rvs star, through the normal sync.runner pipeline like every other archive.'
WHERE archive_code = 'gaia_rvs';

COMMIT;
