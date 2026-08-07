"""SVO CAB stellar libraries (Spanish Virtual Observatory, CAB/INTA-CSIC) — SSA.

Five small, curated, empirical stellar spectral libraries hosted on the same
DaCHS-flavored SVOCat SSA stack at svo2.cab.inta-csic.es/vocats, same
one-archive-many-instruments shape as oirsa.py -- one shared query function
parameterized by sub-collection path, each row labeled by which library it
came from. Confirmed live, 2026-08-07:

  - MILES     (v3/miles)     -- 985 bright reference stars.
  - STELIB    (v2/stelib)    -- 256 stars.
  - XSL       (v3/xshooter)  -- 912 stars (X-Shooter Spectral Library).
  - CaT       (v2/catlib)    -- 696 stars (Ca II Triplet calibration lib).
  - GBS       (gbs)          -- 241 stars (Gaia FGK Benchmark Stars) -- note
    the different path shape confirmed live: "vocats/gbs/ssap.php", not
    "vocats/v3/gbs/...".

This host 302-redirects every request to svocats.cab.inta-csic.es (confirmed
live) -- a bare `curl` without `-L` silently returns an apparently-empty-but-
200 response, easy to mistake for "no data here". `requests` follows
redirects by default so this is a non-issue for this module, but worth
recording since it cost real time to notice.

These are SSA cone-search services, not TAP, but each one's own search-form
help text says "Maximum Search Radius allowed: 180 degrees" -- a genuine
radius, not the IVOA SSA spec's usual "diameter" reading, confirmed live by
querying MILES from two different POS centers (equatorial and polar) with
SIZE=180: identical ~985-row result both times, which a true hemisphere-only
diameter reading could not produce for an all-sky reference-star library.
So a single SIZE=180 query from any POS (0,0 used here) pulls each library's
*entire* catalog in one page -- no pagination, no sky-grid crawl needed at
all, unlike irsa_missions.py's iso_sws/iras_lrs (this project initially
planned for a grid crawl here per the age-old cone-search-only assumption,
but the confirmed-live radius behavior made that unnecessary).

Column names are NOT uniform across the five services (each is configured
independently in SVOCat) -- confirmed live, per collection: the per-row
target-name field is "objname" (MILES, CaT), "name" (STELIB, XSL), or "star"
(GBS); see COLLECTIONS below for the explicit per-collection mapping used
instead of any generic utype-sniffing. Position is uniform, though: every
service exposes a "TargetPos" field (SSA Target.Pos, [ra, dec] in degrees),
confirmed live to be populated on 100% of rows across all five (zero masked
positions in any of them).

No real per-observation date on four of the five: these are static, one-shot
curated libraries (each star observed once, long ago, for the reference
compilation), same "no observation date" shape as feros_gavo.py/rave.py --
confirmed live, no Epoch/mjd-shaped field exists at all for MILES, STELIB,
CaT, or GBS. XSL is the one exception: it carries a real "Epoch" field (MJD)
-- populated on 245 of 912 fits-format rows confirmed live (masked on the
rest, presumably for spectra ingested from the original ESO archive without
a preserved observation date) -- read via clean_float, left None when masked
rather than guessed.

Each real spectrum is served in three parallel formats (VOTable/ASCII/FITS,
"SpecFmt" field) sharing one "AssocID" -- filtered to SpecFmt=='application/
fits' to get one row per real object rather than tripling every count.
CaT additionally serves a paired *error* spectrum under its own AssocID for
every real spectrum (confirmed live: exactly 696 "spec_fits" + 696
"errsp_fits" rows, both formatted application/fits) -- excluded via a
substring check on SpecURL's "errsp"/"spec" label, the same kind of
mime/label-based dedup feros_gavo.py uses for its own paired VOTable/FITS
rows.

archive_url points at "SpecURL" (the real per-row spectrum file link, e.g.
".../miles/ssap.php?ID=0685&label=spec_fits"), not "access_url" (a DataLink
descriptor wrapper around the same file, one extra hop for no benefit here).

No native Gaia column on any of these five (positional/name matching only,
same as most archives here). archive_obs_id is "<collection>:<AssocID>" --
AssocID alone is only unique within one library's own table, not across all
five sharing this one archive_code.

Final, static datasets (none of these five libraries has grown since its
original publication) -- one full pull is enough forever, so fetch() is a
no-op once the cursor marks it done, same shape as rave.py/feros_gavo.py.

webapp/app.py's INSTRUMENT_RESOLVING_POWER/INSTRUMENT_WAVELENGTH_RANGE_NM
deliberately have no entry for Gaia FGK Benchmark Stars -- its own per-row
"instrument" field (visible in the raw SSA response, e.g. "ESPaDOnS_tauCet",
"HARPS.Archive_tauCet", "NARVAL_tauCet") shows this collection is itself a
compilation of high-resolution spectra pulled from several different
underlying spectrographs, not one instrument with one citable resolving
power -- same reasoning several other archives here give for omitting a
mixed/uncertain entry rather than guessing a single number.
"""

import io

import numpy as np
import requests
from astropy.io.votable import parse_single_table
from astropy.time import Time

from sync.base import RawObservation, clean_float

BASE_URL = "http://svo2.cab.inta-csic.es/vocats"

# path: relative to BASE_URL, e.g. "v3/miles" -> .../vocats/v3/miles/ssap.php
# name_field: which column carries the star's name in this collection's own
#   SSA response (not uniform across the five, confirmed live -- see module
#   docstring).
# date_field: only set for XSL, the one collection with a real Epoch column.
# exclude_label_substr: only set for CaT, to drop its paired error spectra.
COLLECTIONS = [
    {"path": "v3/miles", "instrument": "MILES", "name_field": "objname"},
    {"path": "v2/stelib", "instrument": "STELIB", "name_field": "name"},
    {"path": "v3/xshooter", "instrument": "XSL", "name_field": "name", "date_field": "Epoch"},
    {"path": "v2/catlib", "instrument": "CaT", "name_field": "objname", "exclude_label_substr": "errsp"},
    {"path": "gbs", "instrument": "Gaia FGK Benchmark Stars", "name_field": "star"},
]

# Confirmed live (see module docstring): a radius, not a diameter -- 180
# pulls each library's whole catalog in one page, from any center.
QUERY_SIZE = 180

_session = requests.Session()


def _fetch_collection_rows(path: str) -> np.ma.MaskedArray:
    url = f"{BASE_URL}/{path}/ssap.php"
    resp = _session.get(
        url,
        params={"POS": "0,0", "SIZE": QUERY_SIZE, "REQUEST": "queryData"},
        timeout=(15, 120),
    )
    resp.raise_for_status()
    return parse_single_table(io.BytesIO(resp.content)).array


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    if cursor.get("synced_at"):
        return [], cursor

    records = []
    for coll in COLLECTIONS:
        rows = _fetch_collection_rows(coll["path"])
        for row in rows:
            if str(row["SpecFmt"]) != "application/fits":
                continue
            exclude_substr = coll.get("exclude_label_substr")
            if exclude_substr and exclude_substr in str(row["SpecURL"]):
                continue

            name = str(row[coll["name_field"]]).strip()
            if not name:
                continue

            ra, dec = clean_float(row["TargetPos"][0]), clean_float(row["TargetPos"][1])

            obs_date = None
            date_field = coll.get("date_field")
            if date_field:
                mjd = clean_float(row[date_field])
                if mjd is not None:
                    obs_date = Time(mjd, format="mjd").to_datetime().date()

            records.append(
                RawObservation(
                    archive_obs_id=f"{coll['path']}:{row['AssocID']}",
                    archive_url=str(row["SpecURL"]),
                    instrument=coll["instrument"],
                    obs_date=obs_date,
                    ra=ra,
                    dec=dec,
                    raw_target_name=name.replace("_", " "),
                )
            )

    new_cursor = {"synced_at": Time.now().isot, "row_count": len(records)}
    return records, new_cursor
