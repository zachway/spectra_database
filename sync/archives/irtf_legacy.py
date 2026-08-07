"""IRTF Legacy Archive (irtfdata.ifa.hawaii.edu) — pre-2016B, plain HTML GET form.

The third of the four independent IRTF systems identified in irtf_spex.py's
docstring — this one covers 2000B-2016A, before IRTF's holdings moved to
IRSA (irtf_spex.py/irtf_ishell.py, 2016B-present). Originally written off
after only checking the homepage/directory-browser page (real, but pure
HTML directory browsing with no per-observation metadata). Checked again,
directly, per this project's own "verify archive access directly"
precedent — the *other* nav link, /search/, is a real, unauthenticated
GET form (`results.php?Semester=&StartUTCDate=...&EndUTCDate=...&
ProgramID=&Instrument=...`) returning a structured HTML table with real
per-frame ra/dec, target name, instrument, program ID, and a direct file
path — confirmed live, not a directory listing at all.

END_DATE is a hard boundary at 2016-08-01 (exclusive) — irtf_spex.py's IRSA
coverage confirmed live to begin 2016-08-02 (semester 2016B). Without this
cap, the same physical observations from the 2016B transition would risk
appearing under two different archive_code values (this module and
irtf_spex/irtf_ishell) — not a database-level duplicate (archive_code
differs, so the UNIQUE constraint allows it) but a real logical one a user
would see twice in one star's holdings. This module simply never queries
past the boundary, no dedup needed.

Three instrument codes covered, confirmed live via a real 5000-row sample's
own instrument breakdown: sbd_1 (SpeX/"bigdog" v1, 2000-2014) and sbd_2
(SpeX v2, 2014-2016A) are this era's SpeX — walked as two sequential
phases since they're already time-disjoint by the archive's own design,
no need to guess the exact cutover date. cshell (CSHELL, IRTF's other
retired NIR high-res spectrograph, 2000-2016B) turned out to have real,
substantial volume in that same sample (1,190 of 5,000 rows) — added as a
third phase rather than left out, since it's the same table/access
pattern at zero extra cost once the walk itself works. mirsi_1 ("2-20um
camera and grism spectrograph") was also present in that sample but
deliberately excluded — dual imaging/spectroscopy mode with no mode/grism
column in the results table to tell which is which for a given frame,
same reasoning noirlab.py/ing.py give for excluding their own dual-mode
instruments without a clean discriminator. sgd_1/sgd_2 (SpeX's imager/
guider channel, not the spectrograph) and the plain imagers (nsfcam*,
moris*) are never queried at all.

Row granularity is per-raw-FITS-file, not per-sequence (confirmed live:
a single object's one science exposure comes with its own arc/flat
calibration frames as separate rows, object name "Argon lamp"/"Inc lamp"
on those) — no plane/block id exists anywhere in this table to group on
the way _lco_common.py/_irtf_common.py's modern sources do. No hand-
filtering of calibration rows attempted: same reasoning as lick.py/ing.py
give for their own free-text-label archives — "Argon lamp" et al. simply
never match a tracked star's alias list and fall through to a harmless
skip, while a bespoke label-matching filter would inevitably miss some
real variant and risk dropping genuine science rows instead. A handful of
rows carry an obvious placeholder object name ("object name") and blank
ra/dec (confirmed live, e.g. a stray *_noise_test-*.fits engineering
frame) — left in for the same reason, harmless.

ra/dec are real sexagesimal strings on real frames, blank on some
(confirmed live) — parsed the same way as ing.py's own sexagesimal
columns, blank treated as no-position rather than erroring.

Pagination: the form's own page states a 5000-row cap ("Maximum number of
search results returned is 5000"), confirmed live (a one-year, all-
instrument query came back with exactly "Found 5000 records" against an
internal "all rows:5005" comment -- the comment isn't parsed, just the
round-number cap is treated as the truncation signal). Walked as an
adaptive UTC-date-window crawl exactly like ing.py's own (bisect the
window on truncation, grow it back up after a clean page) -- the same
"coverage density varies hugely across a multi-decade archive" reasoning
applies here too, not re-derived independently.
"""

from __future__ import annotations

from datetime import date, timedelta
from urllib.parse import urlencode

import astropy.units as u
import re
import requests
from astropy.coordinates import SkyCoord

from sync.base import RawObservation

BASE_URL = "https://irtfdata.ifa.hawaii.edu"
RESULTS_URL = f"{BASE_URL}/search/results.php"

FIRST_DATE = date(2000, 1, 1)

# Exclusive — irtf_spex.py/irtf_ishell.py's IRSA coverage begins 2016-08-02
# (semester 2016B), confirmed live. See module docstring.
END_DATE = date(2016, 8, 1)

# Sequential phases — see module docstring for why these three and not
# mirsi_1/sgd_1/sgd_2/nsfcam*/moris*.
INSTRUMENTS = ["sbd_1", "sbd_2", "cshell"]
INSTRUMENT_LABELS = {"sbd_1": "SpeX", "sbd_2": "SpeX", "cshell": "CSHELL"}

START_WINDOW_DAYS = 14
MIN_WINDOW_DAYS = 1
MAX_WINDOW_DAYS = 365
MAX_WINDOWS_PER_CALL = 60

TRUNCATION_ROW_COUNT = 5000

_ROW_RE = re.compile(
    r"<tr class='(?:odd|even)-row data'>"
    r"<td align='right'>\s*\d+\s*</td>"
    r"<td align='justify'><a href='(?P<file>[^']*)'>[^<]*</a></td>"
    r"<td align='center'>(?P<instrument>[^<]*)</td>"
    r"<td align='right'>(?P<ra>[^<]*)</td>"
    r"<td align='right'>(?P<dec>[^<]*)</td>"
    r"<td align='center'>(?P<object>[^<]*)</td>"
    r"<td align='center'><a[^>]*>(?P<semester>[^<]*)</a></td>"
    r"<td align='center'>(?P<obsdate>[^<]*)</td>"
    r"<td align='center'>(?P<progid>[^<]*)</td>"
    r"<td align='left'>(?P<observer>[^<]*)</td>"
    r"<td align='left'>(?P<comment>[^<]*)</td>",
    re.S,
)

_session = requests.Session()


def _query(instrument: str, start: date, end: date) -> str:
    params = {
        "Semester": "",
        "StartUTCDate": start.isoformat(),
        "EndUTCDate": end.isoformat(),
        "ProgramID": "",
        "Instrument": instrument,
    }
    resp = _session.get(f"{RESULTS_URL}?{urlencode(params)}", timeout=(15, 120))
    resp.raise_for_status()
    return resp.text


def _parse_coords(ra: str, dec: str) -> tuple[float, float] | tuple[None, None]:
    ra, dec = ra.strip(), dec.strip()
    if not ra or not dec:
        return None, None
    try:
        coord = SkyCoord(ra=ra, dec=dec, unit=(u.hourangle, u.deg))
    except ValueError:
        return None, None
    return coord.ra.deg, coord.dec.deg


def _parse_rows(html: str, instrument_label: str) -> list[RawObservation]:
    records = []
    for m in _ROW_RE.finditer(html):
        try:
            obs_date = date.fromisoformat(m["obsdate"].strip())
        except ValueError:
            obs_date = None
        ra, dec = _parse_coords(m["ra"], m["dec"])
        file_path = m["file"]
        records.append(
            RawObservation(
                archive_obs_id=file_path,
                archive_url=BASE_URL + file_path,
                instrument=instrument_label,
                obs_date=obs_date,
                program_id=m["progid"].strip() or None,
                ra=ra,
                dec=dec,
                raw_target_name=m["object"].strip() or None,
            )
        )
    return records


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    instrument = cursor.get("instrument", INSTRUMENTS[0])
    if instrument == "done":
        return [], cursor

    window_start = date.fromisoformat(cursor["window_start"]) if cursor.get("window_start") else FIRST_DATE
    window_days = cursor.get("window_days", START_WINDOW_DAYS)

    records: list[RawObservation] = []
    windows_scanned = 0

    while window_start < END_DATE and windows_scanned < MAX_WINDOWS_PER_CALL:
        window_end = min(window_start + timedelta(days=window_days - 1), END_DATE - timedelta(days=1))

        while True:
            html = _query(instrument, window_start, window_end)
            page_records = _parse_rows(html, INSTRUMENT_LABELS[instrument])
            if len(page_records) >= TRUNCATION_ROW_COUNT and window_days > MIN_WINDOW_DAYS:
                window_days = max(MIN_WINDOW_DAYS, window_days // 2)
                window_end = min(window_start + timedelta(days=window_days - 1), END_DATE - timedelta(days=1))
                continue
            break

        records.extend(page_records)
        windows_scanned += 1

        window_start = window_end + timedelta(days=1)
        if page_records:
            window_days = min(window_days * 2, MAX_WINDOW_DAYS)
            break
        window_days = min(window_days * 4, MAX_WINDOW_DAYS)

    if window_start >= END_DATE:
        next_index = INSTRUMENTS.index(instrument) + 1
        if next_index < len(INSTRUMENTS):
            new_cursor = {"instrument": INSTRUMENTS[next_index], "window_start": FIRST_DATE.isoformat(), "window_days": START_WINDOW_DAYS}
        else:
            new_cursor = {"instrument": "done"}
    else:
        new_cursor = {"instrument": instrument, "window_start": window_start.isoformat(), "window_days": window_days}

    return records, new_cursor
