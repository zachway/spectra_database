"""Lick / Mt. Hamilton Data Repository -- pure directory browsing, no query API.

https://mthamilton.ucolick.org/data/ has no TAP/REST/form-search endpoint at
all (confirmed live) -- it's a plain per-night directory tree:
    /data/{YYYY-MM}/{DD}/{instrument}/{owner-or-"public"}/
Proprietary nights are named after the PI (password-protected, 401); once
the proprietary period expires the same folder is renamed "public" (see
/data/help/, which documents the folder-rename behavior but -- confirmed
live via a full-text search of that page -- gives no numeric proprietary
period). A GET on a `public/` URL cleanly collapses three different "no
data here" cases into one 404: the day directory doesn't exist (the FAQ
states days without RAID-archived data are simply absent, gaps are normal),
the instrument didn't observe that night, or the night is still proprietary
-- fine for this module's purposes, since all three mean "nothing to fetch
yet, try again later" and 200 means "here's a real, science-or-calibration,
listing" in one request.

Instrument scope: shane + APF only (confirmed live via a day index page:
other listed subfolders that day were allsky/apfcam/hamcam1/hamcam2/
skycam2 -- webcams/acquisition cameras, not spectrographs -- and nickel,
which is Lick's imaging telescope, not spectroscopy). Case-sensitive:
confirmed live that "apf" (lowercase) 404s where "APF" 200s.

Row format confirmed stable 2007-2023 (sampled directly, not assumed):
    <a href="./{filename}">{filename}</a> ... <tt>{comment}  {iso8601}</tt>
{comment} is free text -- sometimes a real target name/catalog number
(HD 84937, HR7906, bare HIP/TYC-style numbers), sometimes a calibration
label (bias, flat, arc, focus, "arc 600/5000 tilt 8400", ...). There's no
separate metadata field distinguishing the two (unlike koa.py's
koaimtyp='object' or lbt.py's imagetyp/dataprod columns) -- so unlike
those archives this module does NOT filter calibration frames server- or
client-side. raw_target_name is passed through as-is for every row and
left to the same generic discover_stars SIMBAD resolution every other
name-based archive here goes through (see ingest.add_star.discover_stars):
"bias"/"flat"/"arc" simply fail to resolve and get skipped downstream,
same outcome a bespoke filter would produce, without hand-guessing which
free-text labels are calibration in every era of this ~20-year archive's
observing-log conventions. Fractional-second digit count varies by era
(confirmed 1 digit in 2007, 2 digits from ~2010 on) -- _ROW_RE's `\\d+`
handles both without caring which era a given night is from.

No ra/dec anywhere in this listing (unlike every TAP-based archive here) --
ra/dec are left None; matching relies entirely on raw_target_name.

Pagination: no field to filter or sort by at all, just a calendar walk.
Cursor is the next calendar date to check. Each fetch() call advances up to
WINDOW_DAYS days (both instruments each), stopping earlier only if it found
zero records AND already reached `today - MIN_AGE_DAYS`, or continuing
PAST WINDOW_DAYS if it found nothing yet and that boundary isn't reached --
same "loop past empty windows so an empty page doesn't fool the generic
stop-on-zero driver" concern gemini.py's own docstring documents for its
fixed-window pagination, adapted here for a calendar walk instead of an
MJD watermark.

MIN_AGE_DAYS (730, ~2yr) exists because a night's proprietary status is NOT
a fixed offset from its observation date -- confirmed live that some nights
were already public within 9-15 months while at least one PI-named folder
from over a decade ago was STILL password-protected (a genuine archive
inconsistency, not this module's bug). Stopping the forward cursor 2 years
short of "today" means the cursor keeps creeping forward in step with real
time rather than freezing at a fixed calendar date, so newly-matured nights
keep getting picked up automatically -- but a night whose proprietary period
somehow outlasts 2 years (like that one decade-old folder) will be missed
once the cursor has moved past it. Accepted tradeoff, not silently swept
away: revisit if a systematic pattern of very-late-expiring nights turns up
(nothing found so far suggests it's more than a rare one-off).

archive_obs_id is instrument+date+filename (no natural ID field exists in
this listing at all) -- unique per file since a given instrument only
produces one listing per calendar night.
"""

from __future__ import annotations

import re
from datetime import date, timedelta

import requests
from bs4 import BeautifulSoup

from sync.base import RawObservation

BASE_URL = "https://mthamilton.ucolick.org/data"
INSTRUMENTS = ["shane", "APF"]  # case-sensitive -- confirmed live

# Repository confirmed live to start between 2006-06 (404) and 2006-09 (200);
# comfortably before that, same "don't bother hunting the exact bound" logic
# carmenes_caha.py's FIRST_DATE uses.
FIRST_DATE = date(2006, 7, 1)

WINDOW_DAYS = 14
MIN_AGE_DAYS = 730

_ROW_RE = re.compile(r"^(.*?)\s+(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)$")

_session = requests.Session()
_session.headers.update({"User-Agent": "Mozilla/5.0"})


def _day_url(day: date, instrument: str) -> str:
    return f"{BASE_URL}/{day:%Y-%m}/{day:%d}/{instrument}/public/"


def _fetch_day(day: date, instrument: str) -> list[dict]:
    url = _day_url(day, instrument)
    try:
        resp = _session.get(url, timeout=30)
    except requests.RequestException:
        return []
    if resp.status_code != 200:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    rows = []
    for tr in soup.find_all("tr"):
        link = tr.find("a", href=lambda h: h and h.startswith("./"))
        tt = tr.find("tt")
        if link is None or tt is None:
            continue
        match = _ROW_RE.match(tt.get_text().strip())
        if match is None:
            continue
        comment, timestamp = match.groups()
        rows.append(
            {
                "filename": link.get_text(strip=True),
                "comment": comment.strip(),
            }
        )
    return rows


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    day = date.fromisoformat(cursor["next_date"]) if cursor.get("next_date") else FIRST_DATE
    boundary = date.today() - timedelta(days=MIN_AGE_DAYS)

    records: list[RawObservation] = []
    days_scanned = 0
    while day <= boundary and (days_scanned < WINDOW_DAYS or not records):
        for instrument in INSTRUMENTS:
            for row in _fetch_day(day, instrument):
                records.append(
                    RawObservation(
                        archive_obs_id=f"{instrument}_{day.isoformat()}_{row['filename']}",
                        archive_url=_day_url(day, instrument) + row["filename"],
                        instrument=f"Lick {instrument}",
                        obs_date=day,
                        ra=None,
                        dec=None,
                        raw_target_name=row["comment"] or None,
                    )
                )
        day += timedelta(days=1)
        days_scanned += 1

    return records, {"next_date": day.isoformat()}
