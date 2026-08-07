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

-- star_id is the internal surrogate PK. Most rows are Gaia-sourced
-- (source_catalog='gaia', gaia_source_id populated) — that's still the
-- primary identifier space everything else in this file assumes. A small
-- number of naked-eye stars too bright for Gaia's detectors (it saturates
-- around G~3; e.g. Arcturus has no Gaia source_id in any release) are
-- instead sourced from the Yale Bright Star Catalogue (BSC5, source_catalog
-- ='bsc5', bsc_hr_number populated instead). Exactly one of the two id
-- columns is set, enforced below — nothing downstream should assume
-- gaia_source_id is non-null.
CREATE TABLE stars (
    star_id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_catalog      TEXT NOT NULL DEFAULT 'gaia' CHECK (source_catalog IN ('gaia', 'bsc5')),
    gaia_source_id      BIGINT UNIQUE,
    bsc_hr_number       INTEGER UNIQUE,   -- Bright Star / Harvard Revised number, e.g. 5340 for Arcturus
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
    added_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    CHECK (
        (source_catalog = 'gaia' AND gaia_source_id IS NOT NULL AND bsc_hr_number IS NULL)
        OR
        (source_catalog = 'bsc5' AND bsc_hr_number IS NOT NULL AND gaia_source_id IS NULL)
    )
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
-- this archive, and where. star_id is nullable so archive records that
-- can't yet be confidently tied to a tracked star still get a row instead of
-- being silently dropped — they sit in needs_review until resolved (manually,
-- or once full LR-based matching is built). References stars.star_id (the
-- surrogate PK), not gaia_source_id directly, since not every tracked star
-- has one (see stars.source_catalog).
CREATE TABLE spectroscopy_holdings (
    id                  BIGSERIAL PRIMARY KEY,
    star_id             BIGINT REFERENCES stars(star_id),
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

    -- Coarse raw-vs-reduced bucket, not the full IVOA ObsCore calib_level
    -- scale (0-3) some archives are derived from (see sync.base.
    -- reduction_status_from_calib_level) — 'unknown' is the honest default
    -- for the many archives here with no calib_level column or other
    -- documented signal to go on (a plain HTML-form/bulk-file/SSA archive
    -- rarely says which processing stage its one download link serves).
    -- Populated today for: mast/mast_jwst/eso/cfht_cadc/dao/gemini/oirsa
    -- (real ObsCore calib_level, confirmed live 2026-08-03), koa (raw-only
    -- per-instrument tables, koa_reduced_data deliberately excluded),
    -- gemini_ghost/gemini_igrins (GOA fetch already filters to reduced
    -- filenames only), naoj (per-exposure product-tier already ranked/
    -- picked), bess (every submission is a wavelength-calibrated 1D
    -- extracted spectrum, not a raw CCD frame -- BeSS's own FAQ confirms
    -- flux specifically is not calibrated, so this is a coarser call than
    -- the ObsCore-derived archives above, but still not "raw" in this
    -- column's 2-way sense), sdss_legacy_optical/sdss_v_optical/
    -- sdss_v_apogee/lamost/lamost_mrs/desi/galah/rave (all 'reduced' --
    -- each is a large pipeline-processed survey whose only public product
    -- is a flux/wavelength-calibrated (or pipeline-combined/coadded) 1D
    -- spectrum; none of these surveys distributes raw CCD frames at all,
    -- confirmed live 2026-08-04 against each module's own deep-link/
    -- reduction-version path), noirlab ('raw' -- its own query hardcodes
    -- proc_type='raw', so every row it returns is an unreduced exposure by
    -- construction). Everywhere else stays 'unknown' until a real
    -- per-archive signal is found.
    reduction_status      TEXT NOT NULL DEFAULT 'unknown'
                             CHECK (reduction_status IN ('raw', 'reduced', 'unknown')),

    -- Retained for needs_review rows (and as an audit trail for matched ones):
    -- the archive's own reported identity/position, independent of our match.
    raw_target_name        TEXT,
    raw_ra                 DOUBLE PRECISION,
    raw_dec                 DOUBLE PRECISION,

    first_seen_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (archive_code, archive_obs_id)
);

CREATE INDEX idx_holdings_star_id ON spectroscopy_holdings (star_id);
CREATE INDEX idx_holdings_archive_status ON spectroscopy_holdings (archive_code, match_status);
CREATE INDEX idx_holdings_needs_review ON spectroscopy_holdings (archive_code) WHERE match_status = 'needs_review';

INSERT INTO archives (archive_code, display_name, access_mechanism, has_native_gaia_column, native_gaia_dr, notes) VALUES
    ('gemini',              'Gemini Observatory Archive',      'tap',       FALSE, NULL, 'Implemented via CADC (ivoa.ObsCore, obs_collection GEMINI/GEMINICADC), not the native REST API. ORDER BY t_min has a severe cliff (72s for 1000 rows) — paginated by 7-day date window instead.'),
    ('gemini_ghost',        'Gemini Observatory Archive — GHOST', 'rest_json', FALSE, NULL, 'GHOST-specific: CADC is missing GHOST''s reduced per-arm spectra (confirmed live), so this goes straight to GOA (archive.gemini.edu) instead. Needs an authenticated session cookie — see sync/archives/gemini_ghost.py.'),
    ('gemini_igrins',       'Gemini Observatory Archive — IGRINS', 'rest_json', FALSE, NULL, 'IGRINS-specific: CADC has zero reduced IGRINS planes at all (confirmed live), so this goes straight to GOA instead, same authenticated pattern as gemini_ghost — see sync/archives/gemini_igrins.py.'),
    ('mast',                'MAST',                             'tap',       FALSE, NULL, 'Implemented against HST, IUE, FUSE via mast.stsci.edu/vo-tap (ivoa.obscore) — a different, working TAP service, not the classic API. IUE/FUSE need access_format=''image/fits'' (not HST''s ''application/fits'') and a client-side dedup by obs_id (many rows per obs_id there, picks the _vo.fits canonical product) — see sync/archives/mast.py. No cliff found. JWST is a separate archive_code (mast_jwst) — its 504 on this same query shape turned out to need bounded-window pagination, not a dead end.'),
    ('mast_jwst',           'MAST — JWST',                      'tap',       FALSE, NULL, 'Same TAP service as mast, split out because JWST needs bounded MJD-window pagination (an unbounded watermark query 504s for this collection specifically, even at TOP 10 — confirmed a genuine server-side cliff, not a row-cap issue) and a per-obs_id product-suffix ranking (_x1d/_s3d/_c1d preferred over intermediate cal/rate/s2d products and unrelated guide-star calibration rows that share the same obs_id) instead of mast.py''s single-suffix _vo.fits dedup — see sync/archives/mast_jwst.py.'),
    ('noirlab',             'NOIRLab Astro Data Archive',       'rest_json', FALSE, NULL, 'Implemented via astroarchive.noirlab.edu (not the datalab.noirlab.edu /tap endpoint, which 404s), covering every dedicated spectrograph on the API: goodman, ghts_blue, ghts_red, chiron, echelle, kosmos, arcoiris, triplespec, cosmos, sami. The 9 beyond goodman were added while the API was down (500s) — same query shape, not independently re-verified live this session, see sync/archives/noirlab.py. Does NOT host DESI (that assumption was wrong — see desi).'),
    ('eso',                 'ESO Science Archive',              'tap',       FALSE, NULL, 'No upload-JOIN support. Implemented, positional match, paginated by t_min watermark.'),
    ('gaia_rvs',            'Gaia RVS',                         'tap',       TRUE,  'dr3', 'sync/archives/gaia_rvs.py queries gaiadr3.gaia_source directly via Gaia''s own TAP service for every source with has_rvs=''true'' (999,645 as of DR3), source_id-watermark paginated — an independent discovery archive like any other, not limited to stars already tracked via some other archive.'),
    ('galah',                'GALAH',                            'tap',       TRUE,  'dr3', 'Implemented — galah_dr4.mainspectable.gaiadr3_source_id, 100% populated.'),
    ('desi',                 'DESI',                             'bulk_file', TRUE,  'dr3', 'Implemented directly against the MWS VAC file (data.desi.lbl.gov) via HTTP range-request streaming — does NOT depend on NOIRLab Data Lab as originally assumed.'),
    ('sdss_v_apogee',        'SDSS-V — APOGEE',                  'cas_sql',   TRUE,  'dr3', 'Implemented — apogeeStar.gaiaedr3_source_id; near-IR, cumulative across SDSS generations.'),
    ('sdss_v_optical',       'SDSS-V — Optical',                 'bulk_file', TRUE,  'dr3', 'Implemented directly against the bulk spAll-lite file (now DR20, ~2.5GB gzip) — GAIA_ID 100% populated for CLASS=STAR in the DR19 sample, including live-confirmed FPS-era rows. DR20 (shipped 2026-07-31) confirmed live via the FITS header that GAIA_ID switched from Gaia DR2 to DR3 source_id, as expected.'),
    ('sdss_legacy_optical',  'SDSS Legacy Optical',              'cas_sql',   FALSE, NULL, 'Implemented — no Gaia column, positional match via specObj ra/dec, capped at MJD 58932.'),
    ('lamost',               'LAMOST',                           'sql_api',   TRUE,  'dr3', 'Implemented via an undocumented SQL API (www.lamost.org/dr11/v2.0/sql/q) — catalogue.gaia_source_id 100% populated for CLASS=STAR. Covers LRS only; MRS is the separate lamost_mrs archive_code.'),
    ('lamost_mrs',           'LAMOST — MRS',                     'sql_api',   TRUE,  'dr3', 'Same undocumented SQL API as lamost, different table (med_combined, the per-target combined-spectrum table behind the "Medium Resolution Catalogue Query" web form — not one of the SQL page''s own documented table names). No CLASS column exists for MRS at all (it''s stars-only by design). obsid is not unique in med_combined (multiple exposures/bands/epochs per target, each its own mobsid) but SELECT DISTINCT on the obsid-level columns collapses that cleanly server-side — confirmed live, ~1000 rows/sec. Deep link (medspectrum/fits/{obsid}) found by brute-force probing since MRS has no lrs_spectrum.js-style readable download link to read the pattern from — see sync/archives/lamost_mrs.py.'),
    ('koa',                  'Keck Observatory Archive',         'tap',       FALSE, NULL, 'Implemented for koa_hires, koa_deimos, koa_esi, koa_lris, koa_nires, koa_nirspec, koa_kpf, koa_mosfire, koa_osiris — schema not uniform across instruments (some use mjd_obs instead of mjd, see sync/archives/koa.py for the per-table map). koa_esi carries real garbage in both mjd and mjd_obs for a majority of rows (confirmed live: 23,283 of 35,102) — filtered via a sanity bound. koa_kcwi/koa_nirc/koa_nirc2/koa_guider/koa_lws/koa_reduced_data checked live and deliberately excluded (extragalactic-dominated, imaging-only, no object metadata, or a different schema shape respectively) — see koa.py docstring.'),
    ('cfht_cadc',             'CFHT / CADC',                      'tap',       FALSE, NULL, 'Implemented via CADC (ivoa.ObsCore, obs_collection CFHT). Real sharp cliff: 20k rows in 11s, 30k in 60s — paginated at 15k.'),
    ('dao',                   'DAO (Dominion Astrophysical Observatory)', 'tap', FALSE, NULL, 'Implemented via the same CADC TAP endpoint as cfht_cadc/gemini (obs_collection DAO) — found during an archive-gap survey, not a new access pattern. Confirmed live: 263,980 spectrum rows, 1986-present. Cliff shape matches CFHT (fast past Gemini''s ~1000-row wall): 10k rows in 2.9s, 20k in 16.9s — paginated at 10k.'),
    ('weave',                 'WEAVE',                            NULL,        FALSE, NULL, 'Not yet public.'),
    ('4most',                 '4MOST',                            NULL,        FALSE, NULL, 'Not yet public; archive confirmed empty, will ride ESO integration once live.'),
    ('rave',                  'RAVE',                             'tap',       TRUE,  'dr3', 'Implemented — III/283/xgaiae3.Gaiae3 via VizieR TAP.'),
    ('carmenes',              'CARMENES',                         'bulk_file', FALSE, NULL, 'Implemented against the GTO DR1 portal — no native Gaia column, resolved via SIMBAD name match (target_name -> alias -> source_id) instead of positional. One holding per star, not per epoch.'),
    ('carmenes_caha',          'CARMENES (CAHA archive, VIS+NIR)', 'html_form', FALSE, NULL, 'Implemented against the general Calar Alto Archive (caha.sdc.cab.inta-csic.es/calto), not DR1 -- covers both channels (DR1''s public zips are VIS-only) across CARMENES''s full operational history (29,379 rows confirmed live), not just the fixed 2016-2020 GTO release. No TAP/API, a plain HTML form POST + table scrape. No native Gaia column, no SIMBAD done here directly (relies on the generic discover_stars path). Some rows share identical ra/dec across different targets (a real display artifact, not a parsing bug) -- raw_target_name is the trustworthy field.'),
    ('lbt',                   'LBT — PEPSI, MODS, LUCI',          'tap',       FALSE, NULL, 'Implemented via a real TAP service at archive.lbto.org/tap (undocumented, found in the portal SPA''s own JS bundle), covering lbt.pepsi, lbt.mods, lbt.luci — not a uniform schema across them (mods/luci isolate spectroscopy via a dataprod column pepsi doesn''t have; luci has no per-target position column at all, uses telra/teldec as a stand-in). object sometimes already reports "Gaia DR3 <id>" directly, parsed opportunistically, but no structured native Gaia column. archive_url points at the general search portal, not a specific file — no direct-file URL exists, only an async bulk-download job system disproportionate to implement for one column.'),
    ('lick',                  'Lick / Mt. Hamilton (Shane + APF)', 'directory_listing', FALSE, NULL, 'Implemented against mthamilton.ucolick.org/data -- pure per-night directory browsing, no TAP/API/form-search at all. Covers shane + APF (nickel is imaging-only, other subfolders are webcams). No ra/dec anywhere in the listing -- name-only match. Cursor walks forward one calendar day at a time (14/call), capped 2 years short of "today" since a night''s proprietary period isn''t a fixed offset (confirmed live: some nights public within 9-15mo, one PI folder still gated after a decade) -- see sync/archives/lick.py for the full tradeoff writeup. No calibration-frame filter (unlike koa.py/lbt.py) -- no metadata field to filter on here, relies on the generic discover_stars SIMBAD step to naturally skip non-stellar labels like bias/flat/arc.'),
    ('feros_gavo',             'FEROS Public Spectra (GAVO)',       'tap',       FALSE, NULL, 'Implemented via dc.g-vo.org/tap (GAVO Heidelberg DaCHS/SSA), found via the reg.g-vo.org registry sweep -- distinct from FEROS data already pulled in via eso.py: this covers FEROS''s 1999 commissioning/guaranteed-time spectra (MJD 51093-51394), entirely before ESO''s own archive coverage starts (earliest FEROS row there is MJD 52955) -- disjoint, not a duplicate. 2,359 real spectra confirmed live. No position column populated at all (confirmed: 0 of 2,359 rows) -- name-only match via ssa_targname. Static dataset, one-shot pull like rave.py.'),
    ('flashheros_gavo',        'Flash/Heros Public Spectra (GAVO)', 'tap',       FALSE, NULL, 'Implemented via the same dc.g-vo.org/tap GAVO Heidelberg hosting as feros_gavo, found in the same registry sweep -- an unrelated late-1990s La Silla bright-star echelle survey (Flash + Heros spectrographs), not affiliated with ESO''s FEROS. 14,573 real spectra confirmed live, real bright-star target names (e.g. "68 Cyg"). No position column populated at all -- name-only match, same as feros_gavo. Static dataset, one-shot pull.'),
    ('asiago',                 'Asiago Observatory (Echelle)',      'tap',       FALSE, NULL, 'Implemented via a real TAP service at archives.ia2.inaf.it/vo/tap/aao (Italy''s IA2 VO center) -- undocumented on the archive''s own portal, found in the underlying app''s JS bundle. Covers aao.ECH (the Echelle spectrograph) only -- aao.AAO (1.49M rows, mostly Schmidt imaging) and aao.AFO (1.06M rows, AFOSC, mixed imaging+spectroscopy with no clean isolating column) deliberately excluded. 41,419 rows confirmed live, 1994-present. RA_RAD/DEC_RAD are radians, not degrees -- converted explicitly. Only 15,505 of 41,419 rows have any position at all (many use a "Manual Coords" placeholder object name instead) -- relies on name matching more than most TAP archives here.'),
    ('harpsn_tng',              'HARPS-N (TNG)',                     'tap',       FALSE, NULL, 'Implemented via the same IA2 TAP infrastructure as asiago (archives.ia2.inaf.it/vo/tap/tng), table tng.TNG_TAP -- an umbrella table across every TNG instrument (7.59M rows), filtered to INSTRUMENT=''HARPN'' AND policy=''FREE'' (the archive''s own public/proprietary field, used directly instead of an estimated embargo period) AND OBJECT != ''NONE'' (calibration frames report RA_RAD=DEC_RAD=0.0 literally, not masked -- filtered at the query level to avoid a false positional match near RA=0/Dec=0). A full COUNT(*) over the unfiltered table times out synchronously; paginated TOP+id-watermark queries come back in ~1.5s for 20,000 rows. RA_RAD/DEC_RAD are radians, same as asiago.'),
    ('elodie',                  'ELODIE (OHP)',                      'html_form', FALSE, NULL, 'Implemented against atlas.obs-hp.fr/elodie (plain HTTP, no TLS) -- no per-object query needed, the CGI returns the entire decommissioned archive (35,535 rows total, confirmed live) as one fixed-width plain-text table when no object filter is given. Half the archive (19,289 rows) is Th-Ar calibration frames, filtered out via an imatyp prefix check, leaving 16,246 real science spectra. Fixed-width parsing required -- naive whitespace-splitting misparses rows with a blank objname or corrupt coordinate field (confirmed live, both occur). Both name and position available (packed J2000 string parsed into ra/dec) -- normal identifier-then-position matching, unlike feros_gavo/flashheros_gavo. Final, decommissioned instrument (last observed 2006) -- one-shot pull.'),
    ('sophie',                  'SOPHIE (OHP)',                      'html_form', FALSE, NULL, 'Implemented against the same OHP host/CGI engine as elodie, but this table has no blank/wildcard bulk dump (confirmed live: both an empty object filter and a bare "%" wildcard return 0 rows) -- paginated instead by iterating a fixed list of common stellar catalog prefixes (HD%, BD%, TYC%, HIP%, GJ%, 2MASS%, Gaia DR%), the archive''s own documented technique for pulling a broad group. Real but incomplete coverage: HD% alone returns 67,714 of the archive''s ~104,105 total rows -- a star cross-matched under a name outside this prefix list will be missed. Same fixed-width parsing and packed-J2000-coordinate shape as elodie. Querying by star name implicitly excludes calibration frames (no separate type filter needed, unlike elodie''s imatyp check).'),
    ('salt_hrs',                'SALT HRS (SAAO SSDA)',              'graphql',   FALSE, NULL, 'Implemented against ssda.saao.ac.za/api -- a GraphQL API, not TAP/VO (the site''s own /tap and /vo/tap routes are just SPA catch-all paths, not real endpoints). Query shape reverse-engineered from the SPA''s own JS bundle -- the `where` arg is a String that must contain a specific JSON filter shape (e.g. {"EQUALS":{"column":"instrument.name","value":"HRS"}}), and `columns` needs exact dotted table.column paths, both only discoverable by grepping the bundle. 47,495 HRS rows confirmed live. No position data at all (confirmed) -- name-only match, same as feros_gavo/flashheros_gavo. Paginated via a GREATER_EQUAL watermark on observation_time.start_time (epoch ms) rather than startIndex alone, since startIndex-only pagination would never notice new observations added after a prior run''s cursor reached the end. Includes embargoed/not-yet-public rows (confirmed the archive lists them) -- archive_url will 403 until each file''s own data_release date passes, same as any other archive''s proprietary content.'),
    ('ing',                     'ING Archive (WHT/ISIS)',            'html_form', FALSE, NULL, 'Implemented against casu.ast.cam.ac.uk/casuadc/ingarch (the old archive.ast.cam.ac.uk is dead) -- a TurboGears web form, no API. Metadata-only by design: bulk file retrieval only exists via a stateful, email-gated async job queue with no way to poll for completion (confirmed live) -- since every archive_url elsewhere in this project already just points at the source archive rather than downloading bytes, archive_url here points at displayHeader?recno=... instead, a real directly-fetchable page, same role the portal link plays for lbt.py. WHT/ISIS only (server-side instrument=ISIS filter, confirmed real substring filtering) -- WHT/ACAM and WHT/LIRIS are dual imaging/spectroscopy instruments with no mode field in the default columns to tell which is which, deliberately excluded. obs_type=TARGET filters out ARC/BIAS/FLAT/SKY calibration. No offset/watermark field exists at all -- paginated via an adaptive nightobs calendar-window walk (bisects on the archive''s own undocumented 1000-row display cap, grows back up after a successful pull) since window size needed varies hugely across ING''s ~40-year history.'),
    ('naoj',                    'NAOJ (Subaru HDS, via JVO)',        'tap',       FALSE, NULL, 'Not the SMOKA archive (still a dead end: registration-gated web wizard, no bulk API) -- implemented against a separate TAP+SSA service run by JVO (jvo.nao.ac.jp/skynode/do/tap/hds/sync) for Subaru''s High Dispersion Spectrograph, found via the reg.g-vo.org registry sweep. A custom JVOQL engine, not DaCHS -- SELECT * is unusable (a malformed access_estsize column declared int but emitting decimal strings crashes the VOTable parser, confirmed live), no instrument_name column (hardcoded to HDS, the table''s only instrument), COUNT(DISTINCT ...) silently ignored (confirmed live), 200,000-row server-side cap regardless of TOP/maxrec. 253,389 rows confirmed live, most raw_ids carrying several pipeline-product rows of the same exposure (fits + text/plain variants) -- deduped per-page by a product-rank preferring the fully-processed 1D fits product, same shape as mast_jwst.py''s per-obs_id ranking. Target name and wavelength range are packed together in obs_title ("NAME [lo:hi]"), parsed apart. No cliff found in TOP+ORDER BY t_mid pagination.'),
    ('oirsa',                   'OIRSA (CfA)',                      'tap',       FALSE, NULL, 'Implemented against a real TAP service at oirsa.cfa.harvard.edu:8080/tap (found via the reg.g-vo.org registry sweep) -- the archive''s own :443 web frontend is a stateful dojo/prototype.js search app with no scriptable API (confirmed live: its /search/* AJAX endpoints 404 for any non-browser client), entirely unrelated to this TAP service. ivoa.obscore unifies all four CfA instruments: FAST (132,452 rows), Hectospec (599,592), Hectochelle (393,267), Echelle (171,278) -- ~1.3M spectra confirmed live, pulled unfiltered since obs_collection is only populated for Echelle (not usable as a discriminator) -- instrument_name read per-row instead just to label each observation. Hectospec/Hectochelle target_name is a plate/configuration id, not a star name, but s_ra/s_dec are still genuine per-fiber target positions (confirmed live: rows sharing one target_name carry different positions, s_fov ~1.5 arcsec) -- positional matching still works even though names don''t resolve. access_url is already a direct per-row file link, no DataLink resolution needed. No cliff found in TOP+ORDER BY t_min pagination up to 50,000 rows/page, same shape as dao.py.'),
    ('gtc',                     'GTC (Gran Telescopio CANARIAS)',    'html_form', FALSE, NULL, 'Previously written off as a dead end (bare GET on searchform.jsp reproducibly 500d) -- that turned out to be the JSP app rejecting requests with no session cookie/User-Agent, not a real server bug; once past that, the actual search (searchres.jsp, POST multipart/form-data) needs no session at all. instCode scopes to spectroscopy-only modes (OSI_LSS, OSI_MOS, MEG_SPE, MEG_IFU, HORuS_SPE, CC_SPE, EMIR_SPE), excluding imaging/polarimetry. 719,927+ products confirmed live. Each row''s Program ID + OBlock ID + numeric ProdId composite key drives FetchProd, a real ungated per-exposure raw-FITS download servlet (confirmed live, no login) -- built by hand for embargoed rows (whose HTML omits the link but still shows the three components), same 403-until-release convention as salt_hrs.py. rpp (page size) accepts values well past the form''s advertised 10/50/100 max but has a real cliff past ~5,000-10,000 rows/page. Default order is newest-first on a live, actively-growing archive (confirmed the "N products found" count changed between two back-to-back requests) -- a page-number/frontier cursor would silently miss new inserts, so this paginates via an adaptive calendar-window walk on the date fields instead (same shape as ing.py), which is immune to that drift.'),
    ('hermes_mercator',         'HERMES (Mercator Telescope, KU Leuven)', 'tap',  FALSE, NULL, 'Added from a user-supplied web form (mercatorvo.ster.kuleuven.be/hermes/q/web/form) -- same GAVO DaCHS/SSA software as feros_gavo/flashheros_gavo (identical ssa_* fields), and like every DaCHS service it also exposes a real TAP endpoint (mercatorvo.ster.kuleuven.be/tap, table hermes.data) instead of needing to scrape the form. Unlike feros_gavo/flashheros_gavo, ssa_location is populated on every one of 119,650 rows confirmed live -- normal identifier-then-position matching applies, not name-only. ssa_instrument is a constant literal "HERMES ()" on every row (hardcoded to "HERMES" here instead). embargo column is always blank -- not usable as an embargo signal, unlike harpsn_tng''s policy field. Paginated via a unique_seqno id watermark (same shape as harpsn_tng/asiago) since the archive is still actively growing, though no cliff was found at this archive''s much smaller scale.'),
    ('bess',                    'BeSS (Be Star Spectra, Observatoire de Paris/OHP)', 'html_form', FALSE, NULL, 'Previously written off as blocked on a real bug (the FITS download button returns HTTP 200 with correct headers but a 0-byte body, confirmed live) -- irrelevant once discovered that a per-spectrum plot PNG at a predictable static path (Spectres_png/S{id:07d}[:3]/sp_{id:07d}.png) is public with no session needed, unlike everything else on this ~20-year-old pure-PHP site, which needs a bootstrap session (Accueil.php then MenuIntro.php) plus a plausible Referer on every request. Consul.php''s own POST search form is a dead end (always returns zero results regardless of input, confirmed with a real resolved star) -- the working path is StarConsul.php (lists all 1506 Be stars that have >=1 spectrum) then Consul.php?specobj=<name> per star. Has real ra/dec (sexagesimal) and target name -- normal identifier-then-position matching applies. No incremental watermark exists (Consul.php''s own pagination cursor is opaque, confirmed non-numeric-offset) -- fetch() does a one-shot full crawl of all 1506 stars, same "static archive, cursor short-circuits to no-op once finished" pattern as rave/galah/elodie/sophie/salt_hrs; a fresh crawl to pick up new amateur uploads needs the cursor cleared by hand. reduction_status is hardcoded ''reduced'' -- every submission is a wavelength-calibrated 1D extracted spectrum, checked against a required FITS format at upload, never a raw CCD frame, though BeSS''s own FAQ notes flux specifically is not calibrated.'),
    ('irtf_spex',               'IRTF SpeX (via IRSA)',              'tap',       FALSE, NULL, 'Implemented via IRSA''s TAP service (irsa.ipac.caltech.edu/TAP), joining caom.observation_irtf + caom.plane_irtf + caom.artifact_irtf (a CAOM2 model split the same way as CADC''s) -- covers only the 2016B-present IRSA-hosted portion of IRTF''s holdings; the pre-2016B IRTF Legacy Archive (irtfdata.ifa.hawaii.edu) is pure HTML directory browsing with no API found, not covered. 86,511 SpeX rows confirmed live, all calibrationlevel=1 (raw) -- no reduced-product tier exists in this table to pick between. No position data at all (confirmed live: every row''s targetposition_coordinates is NULL, no s_ra/s_dec-equivalent column exists either) -- name-only match, target_name cleaned (underscore-to-space, strips an appended "_AV=..." reddening suffix on ~750 rows) before matching. archive_url points at each observation''s real summary.html landing page, not a raw FITS artifact (each plane bundles several raw frames -- flats/arcs/target -- with no single canonical file). Pagination watermarked on plane.time_bounds_lower (MJD), with a client-side same-timestamp planeid-dedup guard -- confirmed live that IRSA''s TAP output rounds this column to 6 decimals for display but compares full precision server-side, so a naive watermark can re-match its own boundary row on the next page. Shares fetch logic with irtf_ishell.py (see sync/archives/_irtf_common.py) -- same tables, only instrument_name differs.'),
    ('irtf_ishell',             'IRTF iSHELL (via IRSA)',            'tap',       FALSE, NULL, 'Same IRSA TAP tables/module as irtf_spex (sync/archives/_irtf_common.py) -- iSHELL was missed in the first investigation pass (Wikipedia''s IRTF page lists it alongside SpeX). 28,126 rows confirmed live, same shape as SpeX in every respect checked: all calibrationlevel=1 (raw), one info/text/html summary.html artifact per plane (1:1, confirmed live), same underscore-joined target_name convention. No position data, name-only match, same as SpeX.'),
    ('irtf_legacy',             'IRTF Legacy Archive',               'html_form', FALSE, NULL, 'The pre-2016B piece of IRTF''s holdings (irtf_spex/irtf_ishell cover 2016B-present via IRSA) -- previously written off after only checking the directory-browser page; the /search/ page turned out to be a real, unauthenticated GET form (results.php) returning a structured HTML table with per-frame ra/dec, target name, program ID, and a direct file path, confirmed live. Covers sbd_1/sbd_2 (SpeX''s two hardware generations across this era, 2000-2016A) and cshell (CSHELL, IRTF''s other retired high-res NIR spectrograph, confirmed live to have real substantial volume -- 1,190 of a 5,000-row sample) -- mirsi_1 (dual imaging/grism-spectroscopy mode, no column to tell which) deliberately excluded, same reasoning noirlab.py/ing.py give for their own dual-mode exclusions. Row granularity is per-raw-FITS-file (confirmed live, e.g. each of a sequence''s own arc/flat calibration frames is its own row, object name literally "Argon lamp"/"Inc lamp") -- no plane/block id exists in this table to group on, and no calibration-frame filter is attempted (same reasoning as lick.py/ing.py: those names simply never match a tracked star and fall through to a harmless skip). Hard-capped at EndUTCDate < 2016-08-01 (exclusive) to avoid the same physical observations appearing under both this archive_code and irtf_spex/irtf_ishell''s. Paginated via an ing.py-style adaptive UTC-date-window crawl against the form''s own confirmed-live 5000-row cap.'),
    ('lco_floyds',              'Las Cumbres Observatory -- FLOYDS',  'rest_json', FALSE, NULL, 'Implemented via the public archive-api.lco.global REST API (unauthenticated, no key needed for public data) -- covers FLOYDS (low-res long-slit, en-coded instruments), confirmed live to be the only spectrograph behind OBSTYPE=SPECTRUM (52,038 public frames). Groups frames by observation_id (BLKUID) and picks the single best-available reduction tier per block (a real basename ending "-1d", else RLEVEL=90, else raw) rather than filtering to one fixed RLEVEL -- confirmed live that most real observations only ever get a raw frame archived (BANZAI-FLOYDS reprocessing lags/never completes for most exposures), so a fixed-RLEVEL filter would have silently dropped the majority of real data. Internal engineering/commissioning frames (proposal_id==''calibrate'' exactly, confirmed live to include a shared bogus 1973 placeholder date and legacy IRAF-stage intermediate RLEVELs like 14/24/35/67/98) are excluded client-side; fetch() loops internally past an all-noise page so this filtering can never look like "caught up" to the runner. Position data is per-frame, not fixed per-instrument (confirmed live: raw/RLEVEL-90 frames carry a real sky-footprint polygon, the final "-1d" product does not) -- ra/dec set from whichever tier was actually picked. archive_url is the API''s own stable per-frame resolver (frames/{id}/), not the embedded `url` field, which is a pre-signed S3 link that expires after 48 hours. Anonymous requests cap page size at exactly 100 (confirmed live: 150+ -> HTTP 400) -- watermarked on observation_date, with a client-side same-timestamp id-dedup guard since the API''s own `start=` filter is inclusive and has no working strict-inequality lookup (confirmed live). Shares grouping/pagination logic with lco_nres.py (see sync/archives/_lco_common.py).'),
    ('lco_nres',                'Las Cumbres Observatory -- NRES',    'rest_json', FALSE, NULL, 'LCO''s second spectrograph (Network of Robotic Echelle Spectrographs, high-res, fiber-fed) -- missed in the first investigation pass, which only found FLOYDS; LCO''s own instruments page lists both. Same shared module as lco_floyds.py (sync/archives/_lco_common.py), identified by OBSTYPE=TARGET rather than SPECTRUM (confirmed live -- TARGET also covers plain Sinistro imaging sharing the same fa-prefixed instrument codes, so OBSTYPE is the real discriminator, not instrument_id). Real reduced fraction is much lower than FLOYDS''s: only 1,034 of 73,200 raw TARGET frames confirmed live to have reached a BANZAI-NRES product at all -- the same per-block best-tier-or-raw-fallback grouping as FLOYDS is what keeps this from dropping ~98% of real NRES observations. Per LCO''s own instruments page, NRES "will not be offered as of semester 2026B" -- winding down, but its historical public spectra stay in scope. Unlike FLOYDS, has real position data on many frames (GeoJSON sky-footprint polygon, confirmed live) -- ra/dec set from the polygon centroid when present, giving a genuine (if imprecise -- confirmed live footprints can be ~10 arcsec across, wider than matcher.py''s default 1" easy-match radius) positional fallback behind identifier matching.'),
    ('subaru_moircs',           'NAOJ (Subaru MOIRCS, via JVO)',      'tap',       FALSE, NULL, 'A second Subaru instrument on the same JVO engine as naoj.py''s HDS endpoint -- naoj.py''s own docstring wrote MOIRCS off as imaging-only based only on a registry SSA/SIA capability check; that was wrong. Real TAP endpoint (jvo.nao.ac.jp/skynode/do/tap/moircs, table public.raw) confirmed live, filtered to obs_mode IN (''SPEC'', ''SPEC_MOS'') to isolate 36,604 real dispersed-spectrum rows from 89,858 imaging rows (obs_mode=IMAG) and other non-science modes. Same JVOQL quirks as HDS -- COUNT(DISTINCT ...) silently ignored (confirmed live), FORMAT=csv ignored, 200,000-row RECORD_MAX cap -- but SELECT * fails for a different reason here (`column t0.center_ra does not exist`, confirmed live, not HDS''s malformed access_estsize column). Unlike HDS, no per-exposure pipeline-product dedup needed -- every row is a genuinely distinct FITS frame from one of MOIRCS'' two separate detector chips (confirmed live via a live GROUP BY data_id HAVING COUNT(*) > 1 returning zero rows). ref_val1/ref_val2 confirmed live to be plain FK5 J2000 RA/Dec in degrees, not a WCS reference needing conversion. Paginated via an id integer watermark, no cliff found (whole 36,604-row set fetched in one ~6.4s query).');


-- =============================================================================
-- Crowdsourced triage for skipped spectroscopy_holdings rows (design sketch).
--
-- No automated heuristic in sync/matcher.py is trustworthy enough to resolve
-- match_status = 'skipped' rows on its own (see the comment on that column
-- above) -- these need a human to look at the raw name/position and a finder
-- chart and pick one of a fixed set of outcomes. A single careless or
-- bad-faith submission attaching the wrong gaia_source_id would be just as
-- damaging as a bad automated heuristic, so submissions do NOT write
-- directly to spectroscopy_holdings/stars. Instead they accumulate here, one
-- row per independent submission, keyed to the holding by
-- (archive_code, archive_obs_id) -- the same pair spectroscopy_holdings
-- already uses as its own UNIQUE constraint -- rather than by `id`, so this
-- table doesn't need to assume anything about spectroscopy_holdings' or
-- stars' primary key shape (a separate, concurrent migration is adding
-- alternate-catalog identifiers to `stars` for Gaia-absent bright stars like
-- Arcturus -- see project notes -- and may change how stars are keyed).
--
-- Applying a submission (updating spectroscopy_holdings / calling
-- ingest.add_star's add_star()/add_star_by_name()) only happens once a
-- quorum of independent submissions agree -- see the TODO on the /triage
-- routes in webapp/app.py for exactly where that stub lives. Left
-- unimplemented here: this table only records raw submissions and whether/
-- when one was applied.
-- =============================================================================

CREATE TABLE skip_classifications (
    id                          BIGSERIAL PRIMARY KEY,

    -- Which skipped holding this submission is about. Deliberately NOT a
    -- foreign key to spectroscopy_holdings.id (a surrogate key this table
    -- shouldn't need to know about) -- (archive_code, archive_obs_id) is the
    -- natural key archive sync code already keys off everywhere else.
    archive_code                TEXT NOT NULL,
    archive_obs_id               TEXT NOT NULL,
    FOREIGN KEY (archive_code, archive_obs_id)
        REFERENCES spectroscopy_holdings (archive_code, archive_obs_id),

    -- The fixed outcomes from the crowdsourced-triage design -- NOT free
    -- text, so submissions stay auditable/aggregable for the quorum step:
    --   attach_gaia_source       -- contributor identified a real Gaia DR3
    --                                source_id for this target (may or may
    --                                not already be tracked in `stars` --
    --                                applying this outcome should go through
    --                                ingest.add_star.add_star(), which
    --                                fetches-and-inserts on demand, see the
    --                                TODO in webapp/app.py).
    --   attach_bright_star        -- contributor identified this as a naked-
    --                                eye star too bright for Gaia to have
    --                                seen at all (saturates at G~3, e.g.
    --                                Arcturus/HR 5340). Tracked via a Bright
    --                                Star (Harvard Revised) catalog number
    --                                instead of a Gaia source_id -- applying
    --                                this outcome should go through
    --                                ingest.add_star.add_bsc_star().
    --   not_a_real_target         -- calibration frame, engineering
    --                                exposure, or other non-target artifact
    --                                -- not a real astronomical object at
    --                                all. Terminal -- should stop
    --                                resurfacing in the queue once applied.
    --   not_a_star                -- a real astronomical object, but not a
    --                                star (galaxy, quasar, Solar System
    --                                body, etc.), so it will never have a
    --                                Gaia source_id or a place in `stars`.
    --                                Terminal, same as not_a_real_target.
    --   confirmed_absent_from_gaia -- a genuine star that just doesn't
    --                                appear in this project's own Gaia-
    --                                sourced `stars` table, and isn't a
    --                                known bright star either -- checked via
    --                                a live cone search against the full
    --                                Gaia DR3 catalog at submission time,
    --                                not just the contributor's say-so, but
    --                                only within gaia_cone_search_radius_arcsec
    --                                of the reported position -- see
    --                                gaia_cone_search_result below.
    outcome                      TEXT NOT NULL CHECK (outcome IN (
                                     'attach_gaia_source',
                                     'attach_bright_star',
                                     'not_a_real_target',
                                     'not_a_star',
                                     'confirmed_absent_from_gaia'
                                 )),

    -- Required for, and only meaningful for, outcome = 'attach_gaia_source'.
    proposed_gaia_source_id       BIGINT,
    CHECK (
        (outcome = 'attach_gaia_source') = (proposed_gaia_source_id IS NOT NULL)
    ),

    -- Required for, and only meaningful for, outcome = 'attach_bright_star'
    -- -- the Yale Bright Star (Harvard Revised) catalog number, resolved via
    -- ingest.add_star.resolve_bsc_hr_number() the same way proposed_gaia_
    -- source_id is resolved for attach_gaia_source.
    proposed_bsc_hr_number        INTEGER,
    CHECK (
        (outcome = 'attach_bright_star') = (proposed_bsc_hr_number IS NOT NULL)
    ),

    -- Required for, and only meaningful for, outcome = 'confirmed_absent_from_gaia'
    -- -- a snapshot of the live Gaia TAP cone-search result the contributor
    -- was shown before confirming (radius + a human-readable summary of what
    -- came back: nothing, or only much-fainter spurious sources), so a later
    -- reviewer/the quorum step doesn't have to trust an unverifiable claim or
    -- re-run the query themselves. See the TODO in webapp/app.py for the
    -- actual TAP call this should reuse (same pattern as
    -- ingest.add_star.resolve_gaia_source_id's GAIA_CONE_QUERY fallback).
    gaia_cone_search_radius_arcsec  REAL,
    gaia_cone_search_result          TEXT,
    CHECK (
        (outcome = 'confirmed_absent_from_gaia') = (gaia_cone_search_result IS NOT NULL)
    ),

    -- No login/auth yet (out of scope for this sketch) -- a plain
    -- self-reported name/handle, trusted at face value.
    submitter                    TEXT NOT NULL,
    note                          TEXT,   -- optional free-form context, not one of the fixed outcomes above

    submitted_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- NULL until the (stubbed) quorum/apply step actually updates
    -- spectroscopy_holdings / calls add_star() for this holding. Kept as a
    -- plain nullable timestamp rather than a status enum since "applied or
    -- not" is the only state this sketch needs -- see open design questions
    -- (quorum size, tie-breaking, conflicting submissions) in the PR notes.
    applied_at                   TIMESTAMPTZ
);

-- Powers both the /triage queue (which skipped holdings still need more
-- submissions before quorum) and the stubbed apply step (tally distinct
-- outcomes per holding among not-yet-applied submissions).
CREATE INDEX idx_skip_classifications_holding
    ON skip_classifications (archive_code, archive_obs_id);

-- Lets a submitter be blocked from voting twice on the same holding rather
-- than silently padding the same outcome -- a submitter changing their mind
-- would need a new row anyway since there's no update path in this sketch
-- (see open design questions).
CREATE UNIQUE INDEX idx_skip_classifications_one_vote_per_submitter
    ON skip_classifications (archive_code, archive_obs_id, submitter);
