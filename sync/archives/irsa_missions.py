"""IRSA space-mission stellar collections — SSA.

Six independent, historical space-mission (or airborne-mission) stellar
spectral collections behind IRSA's one shared SSA service
(irsa.ipac.caltech.edu/SSA?COLLECTION=...), same one-archive-many-
instruments shape as oirsa.py/svo_cab.py. All six confirmed live,
2026-08-07, and share one uniform CAOM-derived SSA column schema (unlike
svo_cab.py's five independently-configured services) -- s_ra/s_dec,
target_name, tmid (MJD), access_url, access_format, calib_level, and
curation_publisherdid are the same column across every collection here.

  - spitzer_sass     -- Spitzer Atlas of Stellar Spectra: 159 stars.
  - spitzer_irs_std  -- Spitzer IRS Standard Stars: 73 stars.
  - iso_sws          -- ISO/SWS.
  - iras_lrs         -- IRAS/LRS Atlas.
  - sofia_exes       -- SOFIA/EXES: 2,580 distinct observations across
    29,212 raw per-order/per-nod FITS files -- ended 2022 (SOFIA retired),
    fully archived, still the single largest collection bundled here.
  - irtf_mearth      -- "Near-Infrared Metallicities, Radial Velocities and
    Spectral Types for 447 Nearby M Dwarfs" (Newton et al. 2014), a
    standalone IRSA table (irtf.mearth_spectra) genuinely separate from the
    IRTF CAOM2 TAP holdings already covered by irtf_spex.py/irtf_ishell.py/
    irtf_legacy.py -- flagged as a known gap in irtf_spex.py's own
    docstring, closed here. 468 stars, 498 spectra.

Deliberately excludes every other Spitzer IRSA collection found alongside
these (spitzer_sings, spitzer_m83m33, spitzer_c2d, spitzer_sage,
spitzer_s5, spitzer_5muses, spitzer_ssgss) -- confirmed extragalactic/ISM
surveys, not stellar, out of scope.

SIZE is a real radius here too, same confirmed-live behavior as svo_cab.py
(not the usual IVOA SSA "diameter" reading): querying irtf_mearth from two
opposite points on the sky (POS=0,90 and POS=180,0) at SIZE=180, and again
at SIZE=360, all returned the identical 498-row result -- consistent only
with SIZE=180 already covering the whole sky, not one hemisphere. Four of
the six collections stay comfortably fast at that whole-sky SIZE=180 despite
their real volume (confirmed live: spitzer_sass 4.7s, spitzer_irs_std 4.4s,
irtf_mearth ~10s, sofia_exes ~20-48s across repeat queries) -- pulled in a
single page each, no pagination needed (see WHOLE_SKY_COLLECTIONS).

iso_sws and iras_lrs are the two exceptions: SIZE=180 hard-times-out
server-side for both ("QUERY_STATUS=ERROR: TransientFault: INTERNAL_SERVER_
ERROR: Job ran but timed out", confirmed live, reproduced twice each) even
though a 2-degree box near Orion only turns up 44/10 real hits -- these two
collections' underlying tables appear to lack a working spatial index (query
latency scales roughly linearly with SIZE, not with area or hit count:
SIZE=20 ~16-18s, SIZE=30 ~27s, SIZE=45 ~48-49s, SIZE=55 already exceeds 75s,
confirmed live for both). SIZE=45 is the largest cell size confirmed to
reliably return within IRSA's apparent timeout window, so these two are
paginated instead via a justified sky-grid crawl (small, historical,
already-closed mission archives -- not a full-sky survey a grid crawl
wouldn't scale to): a fixed 17-cell grid (GRID_CELLS, ~50-degree spacing,
cos(dec)-scaled RA step count) with generous SIZE=45 overlap per cell.
fetch() processes exactly one (collection, cell) pair per call -- same
one-window-per-call shape as ing.py's date-window crawl -- converging after
34 calls (17 cells x 2 collections) plus one final empty call, rather than
one page per collection.

Each real observation is served across 2-3 parallel access_format rows
(a preview image/gif or image/png thumbnail alongside the real science
product) -- filtered per collection to the one real data format (see
WHOLE_SKY_COLLECTIONS/GRID_COLLECTIONS' "format" key), confirmed live per
collection (e.g. spitzer_sass: image/gif + application/fits + text/plain,
159 each; sofia_exes: image/fits + image/png, not evenly split since EXES
emits several raw order/nod files per real observation under one shared
PublisherDID). archive_obs_id is the row's own access_url rather than
curation_publisherdid for exactly that reason -- PublisherDID groups
multiple real distinct files together for sofia_exes, but access_url is
guaranteed unique per real file across every collection here.

target_name is populated directly for iso_sws/sofia_exes/irtf_mearth
(underscore-joined on iso_sws, e.g. "HR4534_BET-LEO" -- cleaned the same
way irtf_spex.py cleans its own underscore-joined names) but is an empty
string on every spitzer_sass/spitzer_irs_std/iras_lrs row (confirmed live)
-- for those three, the name is recovered instead from the tail of
curation_publisherdid (e.g. "ivo://irsa.ipac/spitzer_irs_std/HD 127693" ->
"HD 127693"; "ivo://irsa.ipac/iras_lrs/11210+1707" -> "11210+1707", a real
IRAS Point Source Catalog designation, not a star name -- falls through to
a harmless skip downstream like any other unresolvable identifier).

tmid (MJD) is real and populated on every sofia_exes/irtf_mearth row
(confirmed live, 0 masked in both) but fully masked on every spitzer_sass/
spitzer_irs_std/iso_sws/iras_lrs row confirmed live -- those four are
static, already-fully-reprocessed atlas products with no preserved
per-observation epoch in this table (dataid_date is populated instead, but
that is confirmed live to be a data-*processing*/ingestion timestamp, e.g.
iras_lrs's dataid_date is uniformly "2025-06-04" across every row --
decades after the 1983 IRAS mission -- not a real observation date, so
deliberately not used as a stand-in). obs_date is left None for those four,
same "no real per-observation date" shape as several of svo_cab.py's five
libraries.

calib_level is real and present on every row across all six collections
(confirmed live) -- fed through reduction_status_from_calib_level as usual;
sofia_exes in particular has a genuine mix (2/1/masked observed live),
unlike the other five which are uniformly a single value.
"""

import io
import math

import requests
from astropy.io.votable import parse_single_table
from astropy.time import Time

from sync.base import RawObservation, clean_float, reduction_status_from_calib_level

SSA_URL = "https://irsa.ipac.caltech.edu/SSA"

# Collections whose entire catalog comes back in one whole-sky SIZE=180
# query -- see module docstring for confirmed-live timings/row counts.
WHOLE_SKY_COLLECTIONS = {
    "spitzer_sass": {"instrument": "Spitzer/IRS (SASS)", "format": "application/fits"},
    "spitzer_irs_std": {"instrument": "Spitzer/IRS (Std Stars)", "format": "application/fits"},
    "sofia_exes": {"instrument": "SOFIA/EXES", "format": "image/fits"},
    "irtf_mearth": {"instrument": "IRTF/MEarth", "format": "application/fits"},
}
WHOLE_SKY_QUERY_SIZE = 180

# Collections that time out server-side at that SIZE and are crawled via
# GRID_CELLS instead -- see module docstring.
GRID_COLLECTIONS = {
    "iso_sws": {"instrument": "ISO/SWS", "format": "text/plain"},
    "iras_lrs": {"instrument": "IRAS/LRS", "format": "text/plain"},
}
GRID_CELL_SIZE = 45
GRID_STEP_DEG = 50


def _build_grid(step_deg: float) -> list[tuple[float, float]]:
    """cos(dec)-scaled RA/Dec grid covering the full sky -- fewer RA steps
    near the poles, where a fixed-degree RA step packs cells much closer
    together on the sky than at the equator."""
    cells = []
    dec = -90 + step_deg / 2
    while dec < 90:
        n_ra = max(1, round(360 * math.cos(math.radians(dec)) / step_deg))
        for i in range(n_ra):
            cells.append((i * 360.0 / n_ra, dec))
        dec += step_deg
    return cells


GRID_CELLS = _build_grid(GRID_STEP_DEG)
# Flat (collection, cell_index) work list the grid crawl advances through
# one entry per fetch() call, same one-window-per-call shape as ing.py.
GRID_TASKS = [(name, i) for name in GRID_COLLECTIONS for i in range(len(GRID_CELLS))]

_session = requests.Session()


def _query(collection: str, pos: tuple[float, float], size: float):
    resp = _session.get(
        SSA_URL,
        params={
            "COLLECTION": collection,
            "POS": f"{pos[0]},{pos[1]}",
            "SIZE": size,
            "REQUEST": "queryData",
        },
        timeout=(15, 150),
    )
    resp.raise_for_status()
    return parse_single_table(io.BytesIO(resp.content)).array


def _clean_publisher_did_name(publisher_did: str) -> str:
    """Recovers a target name from curation_publisherdid's own tail segment
    for the collections whose target_name column is blank (see module
    docstring). spitzer_sass wraps its real name in a "sass_<name>_matched"
    convention (confirmed live, e.g. ".../spitzer_sass/sass_HD152386_matched")
    -- stripped here so "HD152386" actually lines up with SIMBAD-style
    aliases, same reasoning irtf_spex.py gives for stripping its own
    appended "_AV=..." suffix. spitzer_irs_std/iras_lrs tails are already
    the bare identifier (e.g. "HD 127693", "11210+1707") and pass through
    unchanged."""
    name = publisher_did.rsplit("/", 1)[-1]
    if name.startswith("sass_"):
        name = name[len("sass_") :]
    if name.endswith("_matched"):
        name = name[: -len("_matched")]
    return name


def _to_observation(row, instrument: str) -> RawObservation:
    # Indexed by position (col_N), not by real field name, because astropy's
    # parse_single_table(...).array confirmed live to fall back to synthetic
    # col_N dtype names for this service's VOTable response -- table.fields[i]
    # .name correctly reports the real names (s_ra, target_name, access_url,
    # ...) at these same positions, but arr.dtype.names does not carry them
    # through. Positions confirmed live and stable across all 6 collections
    # (same shared CAOM-derived schema, see module docstring).
    access_url = str(row["col_18"])
    target_name = str(row["col_27"]).strip()
    if not target_name:
        target_name = _clean_publisher_did_name(str(row["col_8"]))

    tmid = clean_float(row["col_31"])
    obs_date = Time(tmid, format="mjd").to_datetime().date() if tmid is not None else None

    return RawObservation(
        archive_obs_id=access_url,
        archive_url=access_url,
        instrument=instrument,
        obs_date=obs_date,
        ra=clean_float(row["col_0"]),
        dec=clean_float(row["col_1"]),
        raw_target_name=target_name.replace("_", " "),
        reduction_status=reduction_status_from_calib_level(row["col_5"]),
    )


def _fetch_whole_sky() -> list[RawObservation]:
    records = []
    for collection, meta in WHOLE_SKY_COLLECTIONS.items():
        rows = _query(collection, pos=(180, 0), size=WHOLE_SKY_QUERY_SIZE)
        for row in rows:
            if str(row["col_19"]) != meta["format"]:
                continue
            records.append(_to_observation(row, meta["instrument"]))
    return records


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    if not cursor.get("whole_sky_done"):
        records = _fetch_whole_sky()
        new_cursor = {"whole_sky_done": True, "grid_index": 0}
        return records, new_cursor

    grid_index = cursor.get("grid_index", 0)
    if grid_index >= len(GRID_TASKS):
        # Grid fully crawled -- stays a no-op forever after, these are all
        # closed/static historical archives with nothing new to discover.
        return [], cursor

    collection, cell_index = GRID_TASKS[grid_index]
    meta = GRID_COLLECTIONS[collection]
    pos = GRID_CELLS[cell_index]

    rows = _query(collection, pos=pos, size=GRID_CELL_SIZE)
    records = [_to_observation(row, meta["instrument"]) for row in rows if str(row["col_19"]) == meta["format"]]

    new_cursor = dict(cursor)
    new_cursor["grid_index"] = grid_index + 1
    return records, new_cursor
