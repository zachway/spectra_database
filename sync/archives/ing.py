"""ING Archive (WHT/INT/JKT via CASU) -- metadata-only, no file retrieval.

The old archive.ast.cam.ac.uk is dead/decommissioned (confirmed live,
connection refused from two independent networks) -- the live one is
casu.ast.cam.ac.uk/casuadc/ingarch, a TurboGears web-form app with no
TAP/VO/REST API. `displayResults` (POST, multipart/form-data) is the query
endpoint; confirmed live that the `telescope` <select> field must be
present in the POST body even when blank, or the server 500s (a real
TurboGears form-validation quirk, not a documented requirement).

This module deliberately does NOT retrieve actual FITS files -- ING's only
bulk-download path is a stateful, session-tied, email-gated async job
queue (POST recno selections -> POST an email address -> a bare numeric
Job ID is returned -> the archive emails a download link once the job
completes, confirmed live end-to-end with a throwaway address). No
status-check endpoint exists to poll instead of reading email (confirmed:
several guessed endpoints all 404). Since every archive_url in this
project is already just a pointer to where the source archive keeps the
file -- nothing here downloads and stores bytes itself -- there's no need
to build that email/job infrastructure: archive_url instead points at
`displayHeader?recno=...`, a real, directly-fetchable page (no session
needed, confirmed 200) showing that observation's FITS header, the same
role the search-portal link plays for lbt.py (which also has no
direct-file URL).

Spectroscopy only: the `instrument` text field does real server-side
substring filtering (confirmed live: `instrument=ISIS` returns only
WHT/ISIS red/blue arm rows, nothing else) -- used directly rather than
pulling every instrument and filtering client-side. Only WHT/ISIS is
covered. WHT/ACAM and WHT/LIRIS are dual-mode (imaging AND spectroscopy)
and the default result columns don't expose a mode/grism field to tell
which is which for a given exposure -- deliberately excluded rather than
guessing, same reasoning noirlab.py gives for its own excluded tables.
`telescope=WHT` is also passed directly (ISIS only exists there) to keep
result pages smaller.

Real science frames have `obs_type` (a results column, not a form field)
== "TARGET" -- ARC/BIAS/FLAT/SKY calibration frames are excluded via that
check, confirmed live across a real month of data. Some TARGET frames are
engineering pointings (e.g. "FOCRUN-1/9" focus-run sequences) rather than
real stars -- left in rather than hand-filtered further, since those
simply fail to resolve to a tracked star downstream, the same outcome a
bespoke name-based filter would produce (same reasoning lick.py gives for
not hand-filtering its own free-text labels).

Coordinates are real sexagesimal ra/dec on TARGET frames (confirmed live)
but a literal "00:00:00.00 +00:00:00.0" sentinel on calibration/some
engineering frames -- treated as no-position rather than passed through,
same reasoning harpsn_tng.py's OBJECT!='NONE' filter gives for its own
RA=DEC=0.0 sentinel (a real position match near RA=0/Dec=0 would otherwise
be a live risk).

Pagination: no offset/watermark field exists on this form at all -- a
blank/wide query just silently caps at "Displaying only the first 1000"
with no total count and no way to page past it (confirmed live). Walked
instead as an adaptive calendar-window crawl on `nightobs` (which accepts
a real "YYYYMMDD..YYYYMMDD" range, confirmed live), similar in spirit to
lick.py's calendar walk but self-adjusting: a window that comes back
truncated gets bisected (retried at half the size) until it doesn't, and
an accepted window's size grows back up afterward (capped) since coverage
density varies a lot across ING's ~40-year history. Same "keep scanning
until a window finds something, so a real-but-empty window doesn't fool
the generic stop-on-zero driver" concern as lick.py's own docstring
documents, adapted for a variable window size instead of a fixed one.
"""

from __future__ import annotations

import re
from datetime import date, timedelta

import astropy.units as u
import requests
from astropy.coordinates import SkyCoord

from sync.base import RawObservation

BASE_URL = "http://casu.ast.cam.ac.uk/casuadc/ingarch"
RESULTS_URL = f"{BASE_URL}/displayResults"
HEADER_URL = BASE_URL + "/displayHeader?recno={recno}"

# WHT started operations in 1987, but confirmed live that ISIS itself has
# no archived data at all until sometime between 1990 (confirmed empty)
# and 1995 (confirmed real data) -- not worth pinning down further given
# the empty-window growth below skips gaps quickly regardless; same
# "don't bother hunting the exact bound" logic as lick.py's FIRST_DATE.
FIRST_DATE = date(1990, 1, 1)

START_WINDOW_DAYS = 14
MIN_WINDOW_DAYS = 1
MAX_WINDOW_DAYS = 365

# Keep scanning forward within one fetch() call until this many real
# records are found or today is reached -- mirrors lick.py's "days_scanned
# < WINDOW_DAYS or not records" loop condition. Generous because an empty
# multi-year gap (like 1990-1995, confirmed live) needs to be skippable
# within a single call even before window growth ramps up.
MAX_WINDOWS_PER_CALL = 60

ZERO_COORD = "00:00:00.00 +00:00:00.0"

_ROW_RE = re.compile(
    r'<tr>\s*<td><span><input type="checkbox" name="recno" id="[^"]*" value="(?P<recno>[^"]*)"'
    r".*?</a></td>\s*<td>(?P<runno>.*?)</td>\s*<td>(?P<coords>.*?)</td>\s*<td>(?P<objname>.*?)</td>"
    r"\s*<td>(?P<filt>.*?)</td>\s*<td>(?P<exptime>.*?)</td>\s*<td>(?P<ut>.*?)</td>"
    r"\s*<td>(?P<night>.*?)</td>\s*<td>(?P<airmass>.*?)</td>\s*<td>(?P<obstype>.*?)</td>"
    r"\s*<td>(?P<instrument>.*?)</td>",
    re.S,
)

_session = requests.Session()


def _query(start: date, end: date) -> str:
    nightobs = start.strftime("%Y%m%d") if start == end else f"{start:%Y%m%d}..{end:%Y%m%d}"
    response = _session.post(
        RESULTS_URL,
        data={
            "objname": "",
            "coordinates": "",
            "radius": "30",
            "nightobs": nightobs,
            "telescope": "WHT",
            "instrument": "ISIS",
            "waveband": "",
            "pattref": "",
            "runno": "",
        },
        timeout=(15, 180),
    )
    response.raise_for_status()
    return response.text


def _parse_coords(coords: str) -> tuple[float, float] | tuple[None, None]:
    if coords == ZERO_COORD:
        return None, None
    ra_str, dec_str = coords.split(" ", 1)
    coord = SkyCoord(ra=ra_str, dec=dec_str, unit=(u.hourangle, u.deg))
    return coord.ra.deg, coord.dec.deg


def _parse_rows(html: str) -> list[RawObservation]:
    records = []
    seen_recno = set()
    for m in _ROW_RE.finditer(html):
        if m["obstype"].strip() != "TARGET":
            continue
        recno = m["recno"]
        if recno in seen_recno:
            continue
        seen_recno.add(recno)

        night = m["night"].strip()
        try:
            obs_date = date(int(night[0:4]), int(night[4:6]), int(night[6:8]))
        except ValueError:
            obs_date = None

        ra, dec = _parse_coords(m["coords"].strip())

        records.append(
            RawObservation(
                archive_obs_id=recno,
                archive_url=HEADER_URL.format(recno=recno),
                instrument=m["instrument"].strip(),
                obs_date=obs_date,
                ra=ra,
                dec=dec,
                raw_target_name=m["objname"].strip() or None,
            )
        )
    return records


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    window_start = date.fromisoformat(cursor["window_start"]) if cursor.get("window_start") else FIRST_DATE
    window_days = cursor.get("window_days", START_WINDOW_DAYS)
    today = date.today()

    records: list[RawObservation] = []
    windows_scanned = 0

    while window_start <= today and windows_scanned < MAX_WINDOWS_PER_CALL:
        window_end = min(window_start + timedelta(days=window_days - 1), today)

        while True:
            html = _query(window_start, window_end)
            if "Displaying only the first" in html and window_days > MIN_WINDOW_DAYS:
                window_days = max(MIN_WINDOW_DAYS, window_days // 2)
                window_end = min(window_start + timedelta(days=window_days - 1), today)
                continue
            break

        page_records = _parse_rows(html)
        records.extend(page_records)
        windows_scanned += 1

        window_start = window_end + timedelta(days=1)
        if page_records:
            window_days = min(window_days * 2, MAX_WINDOW_DAYS)
            break
        # Empty-but-untruncated window: keep advancing within this same
        # fetch() call rather than returning a zero-record page, which
        # would make the generic stop-on-zero sync driver think the whole
        # archive is exhausted. Grows faster than a successful window
        # (x4 vs x2) so a real multi-year gap gets skipped quickly.
        window_days = min(window_days * 4, MAX_WINDOW_DAYS)

    new_cursor = {"window_start": window_start.isoformat(), "window_days": window_days}
    return records, new_cursor
