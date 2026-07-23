"""NOIRLab Astro Data Archive (dedicated spectrographs) — REST JSON API.

The /tap endpoint 404s — that's a dead end for datalab.noirlab.edu, a
different, unnecessary service. The real, working API is
astroarchive.noirlab.edu, found via its OpenAPI/Swagger schema at
/api/docs/?format=openapi (linked from the docs page's own swagger-ui-init.js,
not from any docs text). POST /api/adv_search/find/, JSON body
{"outfields": [...], "search": [[field, value_or_range], ...]}.

No native Gaia column — positional match. Every returned row already
carries a direct, working `url` field (.../api/retrieve/{md5sum}/) —
confirmed live to a real downloadable FITS file, no separate resolution
step needed.

Field names are non-obvious and had to be reverse-engineered from a live
error message (BADFIELD) that dumped the real available fields: `ra_center`/
`dec_center`/`dateobs_center` (not `ra`/`dec`/`dateobs`), `obs_mode` (not
`obsmode`). `obs_mode` itself is null for Goodman's raw files, so it can't be
used as a spectroscopy filter — instrument identity does that job instead
(each of these is a dedicated spectrograph, no imaging ambiguity).

Originally scoped to just `goodman` (SOAR Goodman Spectrograph); extended to
every other dedicated spectrograph on the same API with the identical query
shape, per this module's own earlier note that it was a trivial swap of the
instrument value — confirmed live, same field names and response shape for
all of them.

No hard row cap found, but slower than most: 20,000 rows took 28.7s.
Paginated at 10,000/page here.

The `["dateobs_center", low, high]` range filter is inclusive on both ends
(confirmed live — no exclusive/">" variant found in the API). Naively
using the watermark as-is for `low` re-fetches the same boundary row on
every page once the cursor catches up to the currently-available data —
confirmed live in production: cursor stuck at the exact same
`last_dateobs` for 150+ consecutive runs, re-matching the same single
record every ~1.5s forever, never converging. Fixed by querying from one
microsecond past the watermark (dateobs_center has microsecond
precision) instead of the watermark itself; the persisted cursor value
is untouched, only the query's lower bound is shifted.

OBJECT (the raw FITS header target name) is fetched and passed through as
raw_target_name, but confirmed live it's frequently NOT a resolvable star
name at all — e.g. "SMC #19 Spec HgAr" (a survey-internal field id, and
"HgAr" flags this specific exposure as an arc-lamp wavelength calibration,
not a science target) or "SMC #21 acq" (a telescope acquisition/pointing
frame). obs_type='object' is supposed to exclude non-science frames but
evidently doesn't catch all of these. Still worth passing through: SIMBAD
resolution already fails gracefully and falls through to positional
matching for anything it can't resolve, so this can only add matches, not
break anything — it's just not the fix for NOIRLab's positional-match rate
that a clean target-name field would have been.

Cursor shape changed from a single flat {"last_dateobs": ...} (one
instrument) to {instrument: {"last_dateobs": ...}, ...} (many). Reads a
pre-existing flat "goodman"-only cursor as a fallback for the "goodman" key
specifically, so a production cursor written before this change doesn't
silently restart goodman from 1990 (UNIQUE (archive_code, archive_obs_id)
would make that harmless, just wasteful — this avoids the waste).

fetch() queries every instrument once per call rather than converging one
at a time — sync.main's driver only stops once a whole page returns zero
records, so an instrument that's already caught up just contributes an
empty result on each subsequent call until every instrument is caught up
together. Simpler than tracking per-instrument exhaustion, and the repeated
empty queries for already-caught-up instruments are cheap relative to the
ones still paging.
"""

from datetime import datetime, timedelta

import requests
from astropy.time import Time

from sync.base import RawObservation, clean_float

FIND_URL = "https://astroarchive.noirlab.edu/api/adv_search/find/"

PAGE_SIZE = 10000

# Every dedicated spectrograph confirmed live on this API with the same
# query shape as goodman (the only one originally wired up).
INSTRUMENTS = [
    "goodman",
    "ghts_blue",
    "ghts_red",
    "chiron",
    "echelle",
    "kosmos",
    "arcoiris",
    "triplespec",
    "cosmos",
    "sami",
]

# Pre-existing production cursors from before multi-instrument support was
# a flat {"last_dateobs": ...} for goodman alone.
_LEGACY_INSTRUMENT = "goodman"


def _next_instant(dateobs: str) -> str:
    dt = datetime.fromisoformat(dateobs.replace("Z", "+00:00")) + timedelta(microseconds=1)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def _last_dateobs(cursor: dict, instrument: str) -> str:
    if instrument in cursor:
        return cursor[instrument].get("last_dateobs", "1990-01-01")
    if instrument == _LEGACY_INSTRUMENT and "last_dateobs" in cursor:
        return cursor["last_dateobs"]
    return "1990-01-01"


def _fetch_instrument(instrument: str, last_dateobs: str) -> tuple[list[RawObservation], str]:
    resp = requests.post(
        f"{FIND_URL}?limit={PAGE_SIZE}&format=json&sort=dateobs_center",
        json={
            "outfields": ["md5sum", "OBJECT", "ra_center", "dec_center", "dateobs_center", "proposal", "url"],
            "search": [
                ["instrument", instrument],
                ["proc_type", "raw"],
                ["obs_type", "object"],
                ["dateobs_center", _next_instant(last_dateobs), "2099-01-01"],
            ],
        },
        timeout=120,
    )
    resp.raise_for_status()
    rows = resp.json()[1:]  # first element is a META/PARAMETERS block, not data

    records = []
    max_dateobs = last_dateobs
    for row in rows:
        dateobs = row["dateobs_center"]
        max_dateobs = max(max_dateobs, dateobs)
        records.append(
            RawObservation(
                archive_obs_id=row["md5sum"],
                archive_url=row["url"],
                instrument=instrument,
                obs_date=Time(dateobs).to_datetime().date(),
                program_id=row.get("proposal"),
                ra=clean_float(row["ra_center"]),
                dec=clean_float(row["dec_center"]),
                raw_target_name=row.get("OBJECT") or None,
            )
        )

    return records, max_dateobs


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    records = []
    new_cursor = {}
    for instrument in INSTRUMENTS:
        last_dateobs = _last_dateobs(cursor, instrument)
        instrument_records, max_dateobs = _fetch_instrument(instrument, last_dateobs)
        records.extend(instrument_records)
        new_cursor[instrument] = {"last_dateobs": max_dateobs}

    return records, new_cursor
