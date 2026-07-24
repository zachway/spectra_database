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
    ('mast',                'MAST',                             'tap',       FALSE, NULL, 'Implemented against HST, IUE, FUSE via mast.stsci.edu/vo-tap (ivoa.obscore) — a different, working TAP service, not the classic API. IUE/FUSE need access_format=''image/fits'' (not HST''s ''application/fits'') and a client-side dedup by obs_id (many rows per obs_id there, picks the _vo.fits canonical product) — see sync/archives/mast.py. No cliff found. JWST is a separate archive_code (mast_jwst) — its 504 on this same query shape turned out to need bounded-window pagination, not a dead end.'),
    ('mast_jwst',           'MAST — JWST',                      'tap',       FALSE, NULL, 'Same TAP service as mast, split out because JWST needs bounded MJD-window pagination (an unbounded watermark query 504s for this collection specifically, even at TOP 10 — confirmed a genuine server-side cliff, not a row-cap issue) and a per-obs_id product-suffix ranking (_x1d/_s3d/_c1d preferred over intermediate cal/rate/s2d products and unrelated guide-star calibration rows that share the same obs_id) instead of mast.py''s single-suffix _vo.fits dedup — see sync/archives/mast_jwst.py.'),
    ('noirlab',             'NOIRLab Astro Data Archive',       'rest_json', FALSE, NULL, 'Implemented via astroarchive.noirlab.edu (not the datalab.noirlab.edu /tap endpoint, which 404s), covering every dedicated spectrograph on the API: goodman, ghts_blue, ghts_red, chiron, echelle, kosmos, arcoiris, triplespec, cosmos, sami. The 9 beyond goodman were added while the API was down (500s) — same query shape, not independently re-verified live this session, see sync/archives/noirlab.py. Does NOT host DESI (that assumption was wrong — see desi).'),
    ('eso',                 'ESO Science Archive',              'tap',       FALSE, NULL, 'No upload-JOIN support. Implemented, positional match, paginated by t_min watermark.'),
    ('gaia_rvs',            'Gaia RVS',                         'tap',       TRUE,  'dr3', 'Native to Gaia itself — trivial join via has_gaia_rvs on stars. Implemented in ingest.add_star.'),
    ('galah',                'GALAH',                            'tap',       TRUE,  'dr3', 'Implemented — galah_dr4.mainspectable.gaiadr3_source_id, 100% populated.'),
    ('desi',                 'DESI',                             'bulk_file', TRUE,  'dr3', 'Implemented directly against the MWS VAC file (data.desi.lbl.gov) via HTTP range-request streaming — does NOT depend on NOIRLab Data Lab as originally assumed.'),
    ('sdss_v_apogee',        'SDSS-V — APOGEE',                  'cas_sql',   TRUE,  'dr3', 'Implemented — apogeeStar.gaiaedr3_source_id; near-IR, cumulative across SDSS generations.'),
    ('sdss_v_optical',       'SDSS-V — Optical',                 'bulk_file', TRUE,  'dr2', 'Implemented directly against the bulk spAll-lite file (612MB gzip) — GAIA_ID 100% populated for CLASS=STAR, including live-confirmed FPS-era rows. Public docs say DR2; internally already DR3, DR20 (~Aug 2026) expected to make that public — recheck then.'),
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
    ('oirsa',                   'OIRSA (CfA)',                      'tap',       FALSE, NULL, 'Implemented against a real TAP service at oirsa.cfa.harvard.edu:8080/tap (found via the reg.g-vo.org registry sweep) -- the archive''s own :443 web frontend is a stateful dojo/prototype.js search app with no scriptable API (confirmed live: its /search/* AJAX endpoints 404 for any non-browser client), entirely unrelated to this TAP service. ivoa.obscore unifies all four CfA instruments: FAST (132,452 rows), Hectospec (599,592), Hectochelle (393,267), Echelle (171,278) -- ~1.3M spectra confirmed live, pulled unfiltered since obs_collection is only populated for Echelle (not usable as a discriminator) -- instrument_name read per-row instead just to label each observation. Hectospec/Hectochelle target_name is a plate/configuration id, not a star name, but s_ra/s_dec are still genuine per-fiber target positions (confirmed live: rows sharing one target_name carry different positions, s_fov ~1.5 arcsec) -- positional matching still works even though names don''t resolve. access_url is already a direct per-row file link, no DataLink resolution needed. No cliff found in TOP+ORDER BY t_min pagination up to 50,000 rows/page, same shape as dao.py.');
