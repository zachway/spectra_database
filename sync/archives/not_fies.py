"""NOT (Nordic Optical Telescope) -- FIES spectrograph, La Palma. HTML form
POST to a bespoke FITS-header archive, no TAP/API.

Real, public, no-login search form at
www.not.iac.es/observing/forms/fitsarchive/ (index.php), POSTing to
query.php on the same host -- confirmed live by fetching the real
`<form name="form1">` and reading its exact hidden/field names rather than
guessing (tbl=FIprihdu,FIexthdu; fk=FIprihdu.`idFIprihdu` =
FIexthdu.`FIprihdu_idFIprihdu`; prihdu=FIprihdu; instrument=FIES). The same
form also covers ALFOSC/MOSCA/NOTCam/StanCam (NOT's imagers and IR camera),
each with its own tbl/fk/prihdu triple selected by a radio button -- only
FIES's is used here.

Two independent, confirmed-live gates, easy to conflate but distinct:

1. query.php itself flatly returns a bare 15-byte "Invalid Request" body
   (200 OK) for any request lacking a real User-Agent and a Referer pointing
   somewhere under this form's own path -- confirmed live: an otherwise-
   correct POST with default request headers still gets rejected; adding a
   browser User-Agent and Referer: .../index.php?instrument=FIES fixes it.
   Same bot-blocking shape as gtc.py's own searchform.jsp/searchres.jsp
   discovery.
2. show.php (the per-file FITS-header detail page each result row links to
   via show.php?f=<filename>) has a separate, much weaker gate: confirmed
   live that ANY Referer merely *containing* the substring "query.php"
   passes -- even a garbage off-domain one like https://evil.com/query.php
   -- while a same-site index.php Referer (or no Referer at all) is
   rejected with "Access Denied". Since a real end user (or this project's
   own webapp) clicking a stored link would never happen to send a
   query.php-shaped Referer, show.php is not a reliable cold link --
   archive_url below points at index.php?instrument=FIES&name=<target>
   instead (confirmed live to reflect a name= GET param straight into the
   form's own objectname field), a real, always-working page in the home
   archive, same "point at a real page, not a link that will mislead"
   convention as lbt.py's own no-direct-file-URL fallback.

criteria=wholesky (one of three radio search modes alongside byobject/
bycoordinates) requires at least one FITS-header filter -- used here with
IMAGETYP = 'OBJECT' (confirmed live real values: 'OBJECT' for genuine
science exposures vs 'WAVE,LAMP' / 'COUNTTEST,LAMP' / etc. for calibration
frames) AND a DATE-OBS range (see pagination below). A small number of
flat-field frames taken via "FIEStool" calibration software still slip
through under IMAGETYP='OBJECT', with object names like "FIEStool flat F4"
and a blank TCSTGT (confirmed live) -- left unfiltered rather than hand-
excluded further, same "simply fails to resolve downstream" reasoning
gtc.py gives for its own unfiltered free-text object names.

No offset/limit field exists on the form at all, and there's a real,
silent, undocumented hard cap of exactly 1000 rows with no truncation
notice (confirmed live: an unbounded 1990-to-today query returns exactly
1000 rows, chronologically ascending from the real archive start; a
narrower one-week window came back well under the cap). Paginated the same
way as ing.py/gtc.py: an adaptive calendar-window walk that bisects the
window on a full (1000-row) page and grows it back up after an unsaturated
one. searchComp[] only offers strict '=' / '>' / '<' / LIKE / NOT LIKE (no
>=/<=, confirmed live from the form's own <select> options) -- each window
is therefore queried as an open interval one day either side of its real
[start, end] boundary (DATE-OBS > day-before AND DATE-OBS < day-after) to
get exact inclusive day coverage without a >= that doesn't exist.

The 12-month proprietary period the form's own page text documents ("headers
of files obtained the most recent 12 months are not visible... Calibration
files are the exception") is real and applies here too, even to
IMAGETYP='OBJECT' rows -- confirmed live: a recent (within ~60 days of
today) OBJECT-filtered window returns only calibration-shaped "FIEStool
flat" rows, no real embargoed science headers.

Row HTML is malformed in a specific, reproducible way: each row's leading
`<a href='show.php?f=...'>` is never closed until that row's very last
`</td>` (confirmed live) -- this breaks a straightforward BeautifulSoup
td-per-tr parse (the unclosed anchor swallows every subsequent sibling row
under Python's plain html.parser, with no lxml/html5lib in this project's
requirements.txt to fall back on). Parsed via a direct regex over the raw
response instead, same "raw regex over the response, not a DOM parser"
workaround as ing.py's own malformed-table handling.

Real per-row fields (confirmed live): FILENAME (root, no extension -- e.g.
"FIGe310097", used directly as archive_obs_id), DATE-OBS, OBJECT (already
clean under the IMAGETYP='OBJECT' filter -- no leftover "ThAr <name>"
contamination seen, unlike the unfiltered table), TCSTGT (the underlying
tracked target name -- sometimes identical to OBJECT, sometimes blank on
the FIEStool-flat rows), RA/DEC (already plain decimal degrees, not
sexagesimal -- no coordinate parsing needed, unlike gtc.py/ing.py),
FIFMSKNM (a real fiber+resolution-mode label, e.g. "F4 HiRes"/"F3 MedRes"/
"F1 LowRes" -- not split out per-record here; see the single resolving-
power range added to webapp/app.py instead), and PROPID (a real proposal-id
format, e.g. "64-302", pulled in via this form's own display[] extra-column
mechanism). A real fraction of OBJECT values are bare Gaia DR3 source_id
strings (e.g. "2203988169026743936") rather than a common name -- passed
through as raw_target_name like any other identifier, not treated as a
structured native Gaia column since it's free text, not a guaranteed-
populated dedicated field.

Archive coverage starts 2006-11-10 (confirmed live: the earliest row of an
unbounded ascending pull) -- FIRST_DATE below is a round conservative bound
before that, same "not worth pinning down further, the empty-window growth
skips any gap quickly regardless" reasoning as gtc.py/ing.py's own
FIRST_DATE picks.

No native Gaia column, no calib_level-equivalent field -- reduction_status
left unset, same reasoning as chandra.py/naoj.py's own raw-FITS-only
archives.
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from urllib.parse import quote_plus

import requests

from sync.base import RawObservation

BASE_URL = "https://www.not.iac.es/observing/forms/fitsarchive"
QUERY_URL = f"{BASE_URL}/query.php"
NAME_SEARCH_URL = BASE_URL + "/index.php?instrument=FIES&name={name}&ra=&dec=&radius=&radiusunit="

# query.php's own bot-block requires a real User-Agent and a Referer under
# this form's own path -- see module docstring point 1. Any page under
# fitsarchive/ works; index.php?instrument=FIES is the real one a browser
# would have sent.
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SpectraPointer/1.0)",
    "Referer": f"{BASE_URL}/index.php?instrument=FIES",
}

_session = requests.Session()

# Real, silent, undocumented cap confirmed live -- see module docstring.
PAGE_CAP = 1000

FIRST_DATE = date(2000, 1, 1)

START_WINDOW_DAYS = 14
MIN_WINDOW_DAYS = 1
MAX_WINDOW_DAYS = 365

# Mirrors ing.py/gtc.py's own per-call cap -- lets one fetch() call skip a
# long empty/sparse stretch of history without the generic stop-on-zero
# driver concluding the whole archive is exhausted.
MAX_WINDOWS_PER_CALL = 60

# Each row: <tr><td class=row[12]><a href='show.php?f=ID'>NAME.fits</td>
# ...(9 or 10 more <td class=row[12]>value</td>, the very last one carrying
# a stray, never-explicitly-opened </a> right before its </td>)...</tr>.
# See module docstring for why this is parsed via regex rather than a DOM
# parser.
_ROW_RE = re.compile(
    r"<tr><td class=row[12]><a href='show\.php\?f=(?P<obs_id>[^']+)'>[^<]*</td>"
    r"(?P<rest>.*?)</tr>",
    re.S,
)
_FIELD_RE = re.compile(r"<td class=row[12]>(.*?)</td>", re.S)

# Column order after FILENAME, matching the exact request built in _query
# below: the form's own default columns, plus PROPID appended via display[].
_COL_DATE_OBS = 0
_COL_OBJECT = 1
_COL_TCSTGT = 2
_COL_RA = 3
_COL_DEC = 4
_COL_PROPID = 10


def _query(window_start: date, window_end: date) -> str:
    # searchComp[] has no >=/<= (confirmed live, see docstring) -- queried
    # as an open interval one day either side of the real boundary so each
    # window covers its [window_start, window_end] days exactly once.
    after = (window_start - timedelta(days=1)).isoformat()
    before = (window_end + timedelta(days=1)).isoformat()

    data = [
        ("tbl", "FIprihdu,FIexthdu"),
        ("fk", "FIprihdu.`idFIprihdu` = FIexthdu.`FIprihdu_idFIprihdu`"),
        ("prihdu", "FIprihdu"),
        ("instrument", "FIES"),
        ("criteria", "wholesky"),
        ("objectname", ""),
        ("resolve", "resolve"),
        ("ra", ""),
        ("dec", ""),
        ("radius", "1"),
        ("radiusunit", "arcmin"),
        ("searchField[]", "FIprihdu.`IMAGETYP`"),
        ("searchComp[]", "="),
        ("searchValue[]", "OBJECT"),
        ("searchConjunction[]", "AND"),
        ("searchField[]", "FIprihdu.`DATE-OBS`"),
        ("searchComp[]", ">"),
        ("searchValue[]", after),
        ("searchConjunction[]", "AND"),
        ("searchField[]", "FIprihdu.`DATE-OBS`"),
        ("searchComp[]", "<"),
        ("searchValue[]", before),
        ("searchConjunction[]", " "),
        ("searchField[]", "FIprihdu.`ADC1ANG`"),
        ("searchComp[]", "="),
        ("searchValue[]", ""),
        ("searchConjunction[]", " "),
        ("searchField[]", "FIprihdu.`ADC1ANG`"),
        ("searchComp[]", "="),
        ("searchValue[]", ""),
        ("display[]", "FIprihdu.`PROPID` AS 'PROPID'"),
        ("submit", " Search Archive "),
    ]
    resp = _session.post(QUERY_URL, data=data, headers=_HEADERS, timeout=(15, 90))
    resp.raise_for_status()
    resp.encoding = "iso-8859-1"
    return resp.text


def _parse_rows(html: str) -> list[RawObservation]:
    records = []
    for m in _ROW_RE.finditer(html):
        fields = [re.sub(r"</a>$", "", f) for f in _FIELD_RE.findall(m.group("rest"))]

        obs_id = m.group("obs_id")
        date_obs = fields[_COL_DATE_OBS]
        try:
            obs_date = date.fromisoformat(date_obs[:10])
        except ValueError:
            obs_date = None

        object_name = fields[_COL_OBJECT].strip()
        tcstgt = fields[_COL_TCSTGT].strip()
        raw_target_name = object_name or tcstgt or None

        try:
            ra = float(fields[_COL_RA])
            dec = float(fields[_COL_DEC])
        except (ValueError, IndexError):
            ra = dec = None

        program_id = None
        if len(fields) > _COL_PROPID:
            propid = fields[_COL_PROPID].strip()
            program_id = propid or None

        name_for_url = raw_target_name or obs_id
        archive_url = NAME_SEARCH_URL.format(name=quote_plus(name_for_url))

        records.append(
            RawObservation(
                archive_obs_id=obs_id,
                archive_url=archive_url,
                instrument="FIES",
                obs_date=obs_date,
                program_id=program_id,
                ra=ra,
                dec=dec,
                raw_target_name=raw_target_name,
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
            page_records = _parse_rows(html)
            if len(page_records) >= PAGE_CAP and window_days > MIN_WINDOW_DAYS:
                window_days = max(MIN_WINDOW_DAYS, window_days // 2)
                window_end = min(window_start + timedelta(days=window_days - 1), today)
                continue
            break

        records.extend(page_records)
        windows_scanned += 1

        window_start = window_end + timedelta(days=1)
        if page_records:
            window_days = min(window_days * 2, MAX_WINDOW_DAYS)
            break
        # Empty-but-untruncated window: keep scanning within this same
        # fetch() call instead of returning a zero-record page, which would
        # make the generic stop-on-zero sync driver think the whole archive
        # is exhausted -- same concern ing.py/gtc.py document.
        window_days = min(window_days * 4, MAX_WINDOW_DAYS)

    new_cursor = {"window_start": window_start.isoformat(), "window_days": window_days}
    return records, new_cursor
