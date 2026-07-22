-- Multi-Archive Spectroscopy Cross-Match Database
-- Lean "pointer" model: we store just enough per observation to let a user
-- click through to the spectrum in its home archive. No archive metadata
-- mirrors, no full likelihood-ratio match machinery yet (deferred — see
-- match_method/match_status below for the interim "easy match" approach).

-- q3c powers sync.matcher's positional-match candidate lookup (indexed
-- radial queries against stars.ra/dec) — not packaged for conda-forge or
-- Homebrew as of this writing, must be built from source:
-- https://github.com/segasai/q3c
CREATE EXTENSION IF NOT EXISTS q3c;

CREATE TABLE stars (
    gaia_source_id      BIGINT PRIMARY KEY,
    ra                  DOUBLE PRECISION NOT NULL,   -- deg, ICRS, at ref_epoch
    dec                 DOUBLE PRECISION NOT NULL,   -- deg, ICRS, at ref_epoch
    ref_epoch           DOUBLE PRECISION NOT NULL DEFAULT 2016.0,
    pmra                DOUBLE PRECISION,            -- mas/yr
    pmdec               DOUBLE PRECISION,            -- mas/yr
    parallax            REAL,                        -- mas
    phot_g_mean_mag     REAL,
    phot_bp_mean_mag    REAL,
    phot_rp_mean_mag    REAL,
    has_gaia_rvs        BOOLEAN NOT NULL DEFAULT FALSE,
    -- Flag only, same free column on the gaia_source row — actual XP spectra
    -- are not ingested/stored (deferred, see project notes on storage).
    has_xp_continuous   BOOLEAN NOT NULL DEFAULT FALSE,
    -- What the caller actually searched for, when ingestion went through name
    -- resolution (SIMBAD) rather than a known source_id. NULL if added directly.
    input_name          TEXT,
    -- SIMBAD's full alias list for this star (catalog IDs, common names, ...),
    -- cached at add_star time. Used to identifier-match an archive's own
    -- target_name against this star before falling back to positional
    -- matching — identifier match is the primary path, position is backup,
    -- since Gaia's astrometric fit can be biased for binaries/crowded fields
    -- in ways that break pure positional matching even with correct PM.
    name_aliases         TEXT[],
    added_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Powers sync.matcher's positional-match candidate lookup (q3c_join against
-- this) — without it, positional matching has to load the whole tracked-star
-- catalog into Python and rebuild a KD-tree per observation epoch, which
-- stopped scaling once the catalog passed ~1M rows (confirmed live: single
-- pages of date-heavy archives like ESO/MAST took minutes to over an hour).
CREATE INDEX q3c_stars_idx ON stars (q3c_ang2ipix(ra, dec));

CREATE TABLE archives (
    archive_code            TEXT PRIMARY KEY,   -- e.g. 'gemini', 'sdss_v_optical', 'carmenes'
    display_name            TEXT NOT NULL,
    access_mechanism        TEXT,               -- 'tap' | 'rest_json' | 'bulk_file' | 'cas_sql' | ...
    has_native_gaia_column  BOOLEAN NOT NULL DEFAULT FALSE,
    -- Which Gaia data release the archive's own source_id column is expressed in, if known.
    -- SDSS-V's bulk spAll GAIA_ID is 'dr2' today; expected to become 'dr3' when DR20 ships
    -- (~Aug 2026) — recheck and update this row when that happens.
    native_gaia_dr           TEXT,
    notes                    TEXT,
    added_at                 TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per archive per sync run's progress. sync_cursor is JSONB because
-- each archive paginates differently (date windows, offsets, run2d/run1d
-- generations, ...) — no single scalar watermark fits all of them.
CREATE TABLE archive_sync_state (
    archive_code        TEXT PRIMARY KEY REFERENCES archives(archive_code),
    sync_cursor          JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_run_at          TIMESTAMPTZ,
    last_run_status       TEXT CHECK (last_run_status IN ('success', 'partial', 'failed')),
    last_run_notes        TEXT,
    rows_seen_last_run    INTEGER
);

-- The core deliverable table: does spectroscopic data exist for this star in
-- this archive, and where. gaia_source_id is nullable so archive records that
-- can't yet be confidently tied to a tracked star still get a row instead of
-- being silently dropped — they sit in needs_review until resolved (manually,
-- or once full LR-based matching is built).
CREATE TABLE spectroscopy_holdings (
    id                  BIGSERIAL PRIMARY KEY,
    gaia_source_id      BIGINT REFERENCES stars(gaia_source_id),
    archive_code        TEXT NOT NULL REFERENCES archives(archive_code),
    archive_obs_id      TEXT NOT NULL,   -- archive-native observation/dataset ID
    archive_url         TEXT NOT NULL,   -- deep link back to the archive's own UI
    instrument          TEXT,
    obs_date            DATE,
    program_id          TEXT,

    match_method        TEXT NOT NULL CHECK (match_method IN (
                             'direct_gaia_column',   -- archive already carries Gaia source_id
                             'name_resolved',         -- archive's target_name matched a tracked star's SIMBAD alias
                             'positional_easy_match', -- tight-radius, single-candidate match
                             'lr_matched',            -- full likelihood-ratio match (not built yet)
                             'manual'
                         )),
    -- skipped: no candidate at all (unlike needs_review's 2+ candidates) —
    -- persisted (not just counted and discarded) so the raw report can be
    -- reviewed later, e.g. for crowd-sourced manual attachment to a star.
    match_status         TEXT NOT NULL CHECK (match_status IN ('matched', 'needs_review', 'rejected', 'skipped')),
    theta_arcsec          REAL,   -- separation for positional matches; null for direct-column matches

    -- Retained for needs_review rows (and as an audit trail for matched ones):
    -- the archive's own reported identity/position, independent of our match.
    raw_target_name        TEXT,
    raw_ra                 DOUBLE PRECISION,
    raw_dec                 DOUBLE PRECISION,

    first_seen_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (archive_code, archive_obs_id)
);

CREATE INDEX idx_holdings_gaia_source_id ON spectroscopy_holdings (gaia_source_id);
CREATE INDEX idx_holdings_archive_status ON spectroscopy_holdings (archive_code, match_status);
CREATE INDEX idx_holdings_needs_review ON spectroscopy_holdings (archive_code) WHERE match_status = 'needs_review';

INSERT INTO archives (archive_code, display_name, access_mechanism, has_native_gaia_column, native_gaia_dr, notes) VALUES
    ('gemini',              'Gemini Observatory Archive',      'tap',       FALSE, NULL, 'Implemented via CADC (ivoa.ObsCore, obs_collection GEMINI/GEMINICADC), not the native REST API. ORDER BY t_min has a severe cliff (72s for 1000 rows) — paginated by 7-day date window instead.'),
    ('gemini_ghost',        'Gemini Observatory Archive — GHOST', 'rest_json', FALSE, NULL, 'GHOST-specific: CADC is missing GHOST''s reduced per-arm spectra (confirmed live), so this goes straight to GOA (archive.gemini.edu) instead. Needs an authenticated session cookie — see sync/archives/gemini_ghost.py.'),
    ('gemini_igrins',       'Gemini Observatory Archive — IGRINS', 'rest_json', FALSE, NULL, 'IGRINS-specific: CADC has zero reduced IGRINS planes at all (confirmed live), so this goes straight to GOA instead, same authenticated pattern as gemini_ghost — see sync/archives/gemini_igrins.py.'),
    ('mast',                'MAST',                             'tap',       FALSE, NULL, 'Implemented against HST via mast.stsci.edu/vo-tap (ivoa.obscore) — a different, working TAP service, not the classic API. No cliff found. JWST hits a genuine 504 on the same query shape — not yet covered.'),
    ('noirlab',             'NOIRLab Astro Data Archive',       'rest_json', FALSE, NULL, 'Implemented via astroarchive.noirlab.edu (not the datalab.noirlab.edu /tap endpoint, which 404s), covering every dedicated spectrograph on the API: goodman, ghts_blue, ghts_red, chiron, echelle, kosmos, arcoiris, triplespec, cosmos, sami. The 9 beyond goodman were added while the API was down (500s) — same query shape, not independently re-verified live this session, see sync/archives/noirlab.py. Does NOT host DESI (that assumption was wrong — see desi).'),
    ('eso',                 'ESO Science Archive',              'tap',       FALSE, NULL, 'No upload-JOIN support. Implemented, positional match, paginated by t_min watermark.'),
    ('gaia_rvs',            'Gaia RVS',                         'tap',       TRUE,  'dr3', 'Native to Gaia itself — trivial join via has_gaia_rvs on stars. Implemented in ingest.add_star.'),
    ('galah',                'GALAH',                            'tap',       TRUE,  'dr3', 'Implemented — galah_dr4.mainspectable.gaiadr3_source_id, 100% populated.'),
    ('desi',                 'DESI',                             'bulk_file', TRUE,  'dr3', 'Implemented directly against the MWS VAC file (data.desi.lbl.gov) via HTTP range-request streaming — does NOT depend on NOIRLab Data Lab as originally assumed.'),
    ('sdss_v_apogee',        'SDSS-V — APOGEE',                  'cas_sql',   TRUE,  'dr3', 'Implemented — apogeeStar.gaiaedr3_source_id; near-IR, cumulative across SDSS generations.'),
    ('sdss_v_optical',       'SDSS-V — Optical',                 'bulk_file', TRUE,  'dr2', 'Implemented directly against the bulk spAll-lite file (612MB gzip) — GAIA_ID 100% populated for CLASS=STAR, including live-confirmed FPS-era rows. Public docs say DR2; internally already DR3, DR20 (~Aug 2026) expected to make that public — recheck then.'),
    ('sdss_legacy_optical',  'SDSS Legacy Optical',              'cas_sql',   FALSE, NULL, 'Implemented — no Gaia column, positional match via specObj ra/dec, capped at MJD 58932.'),
    ('lamost',               'LAMOST',                           'sql_api',   TRUE,  'dr3', 'Implemented via an undocumented SQL API (www.lamost.org/dr11/v2.0/sql/q) — catalogue.gaia_source_id 100% populated for CLASS=STAR.'),
    ('koa',                  'Keck Observatory Archive',         'tap',       FALSE, NULL, 'Implemented for koa_hires, koa_deimos, koa_esi, koa_lris, koa_nires — the latter two use mjd_obs instead of mjd, schema not uniform across instruments. koa_esi carries real garbage in both mjd and mjd_obs for a majority of rows (confirmed live: 23,283 of 35,102) — filtered via a sanity bound, see sync/archives/koa.py.'),
    ('cfht_cadc',             'CFHT / CADC',                      'tap',       FALSE, NULL, 'Implemented via CADC (ivoa.ObsCore, obs_collection CFHT). Real sharp cliff: 20k rows in 11s, 30k in 60s — paginated at 15k.'),
    ('dao',                   'DAO (Dominion Astrophysical Observatory)', 'tap', FALSE, NULL, 'Implemented via the same CADC TAP endpoint as cfht_cadc/gemini (obs_collection DAO) — found during an archive-gap survey, not a new access pattern. Confirmed live: 263,980 spectrum rows, 1986-present. Cliff shape matches CFHT (fast past Gemini''s ~1000-row wall): 10k rows in 2.9s, 20k in 16.9s — paginated at 10k.'),
    ('weave',                 'WEAVE',                            NULL,        FALSE, NULL, 'Not yet public.'),
    ('4most',                 '4MOST',                            NULL,        FALSE, NULL, 'Not yet public; archive confirmed empty, will ride ESO integration once live.'),
    ('rave',                  'RAVE',                             'tap',       TRUE,  'dr3', 'Implemented — III/283/xgaiae3.Gaiae3 via VizieR TAP.'),
    ('carmenes',              'CARMENES',                         'bulk_file', FALSE, NULL, 'Implemented against the GTO DR1 portal — no native Gaia column, resolved via SIMBAD name match (target_name -> alias -> source_id) instead of positional. One holding per star, not per epoch.'),
    ('lbt',                   'LBT — PEPSI',                      'tap',       FALSE, NULL, 'Implemented via a real TAP service at archive.lbto.org/tap (undocumented, found in the portal SPA''s own JS bundle), lbt.pepsi table only — mods/luci (also spectroscopy-capable) not yet added. object sometimes already reports "Gaia DR3 <id>" directly, parsed opportunistically, but no structured native Gaia column. archive_url points at the general search portal, not a specific file — no direct-file URL exists, only an async bulk-download job system disproportionate to implement for one column.');
