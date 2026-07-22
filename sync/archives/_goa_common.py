"""Shared authenticated-GOA fetch logic for gemini_ghost.py and gemini_igrins.py.

Both instruments need the same GOA_SESSION_COOKIE auth, the same jsonsummary
date-windowed pagination (looping past empty windows internally, same reason
as gemini.py's CADC-based fetch -- an empty window doesn't mean the archive
is exhausted), and the same clear-error-on-auth-failure behavior. Only the
instrument name, the earliest date, and the reduced-product filename filter
differ per instrument -- see gemini_ghost.py and gemini_igrins.py for those
and the live evidence behind each one's filter choice.

jsonsummary silently caps its response at 2000 rows -- confirmed live: a
180-day IGRINS window returned exactly 2000 rows, all from the first few
days, never reaching the reduced files known to exist later in that same
window (see gemini_igrins.py). A too-wide window doesn't error, it just
quietly drops data past row 2000 -- worse than an empty page, since it
looks like a normal partial result.

A single fixed WINDOW_DAYS isn't safe against this -- confirmed live twice:
even after shrinking IGRINS from 180 to 7 days, a real dense stretch
(20180422-20180429, right after a ~190-exposure two-day burst) still hit
the cap. Observing density varies a lot day to day and instrument to
instrument, so instead of guessing yet another fixed number, a capped
response here halves the window and retries the *same* window_start,
down to a 1-day floor, before giving up and raising. Each new window_start
resets back to the caller's preferred window_days -- if density was just a
temporary burst, this avoids staying artificially narrow (and slow) for
the rest of the scan.

Confirmed live a third time: a single IGRINS day (2021-08-04, right around
when IGRINS moved from Gemini-South to Gemini-North) still hit the cap even
at the 1-day floor -- there's no coarser window to blame this time, the day
itself has too much. Raising here would block the entire rest of the scan
on one bad day, indefinitely, with no way to skip past it short of editing
code. Logs a warning and moves on instead (same shape as ingest.add_star's
"SIMBAD resolution failed, continuing without it" precedent) -- whatever
spec_a0v.fits files happen to be within that day's first ~2000 rows still
get processed, only rows past the cap for that specific day are missed.

Also: GOA serves these files bzip2-compressed (filenames end in .fits.bz2,
not .fits) -- confirmed live. Callers' is_reduced() should check for a
substring, not an exact suffix match, or it'll never match anything.
"""

import logging
import os
from datetime import date, datetime, timedelta
from typing import Callable

import requests

from sync.base import RawObservation

logger = logging.getLogger(__name__)

BASE_URL = "https://archive.gemini.edu"
DOWNLOAD_URL = BASE_URL + "/file/{filename}"

COOKIE_ENV_VAR = "GOA_SESSION_COOKIE"

# Confirmed live (2026-07-22): a real jsonsummary response hit exactly this
# count with more data known to exist beyond it. Not documented anywhere,
# just observed.
RESPONSE_ROW_CAP = 2000


def _date_str(d: date) -> str:
    return d.strftime("%Y%m%d")


def fetch_reduced(
    cursor: dict,
    instrument: str,
    first_date: date,
    window_days: int,
    is_reduced: Callable[[str], bool],
) -> tuple[list[RawObservation], dict]:
    cookie = os.environ.get(COOKIE_ENV_VAR)
    if not cookie:
        raise RuntimeError(
            f"{COOKIE_ENV_VAR} not set. Log into {BASE_URL} (ORCID login recommended), "
            "copy the gemini_archive_session cookie value from the post-login page, and "
            f"set {COOKIE_ENV_VAR} to it before running the {instrument.lower()} archive."
        )
    cookies = {"gemini_archive_session": cookie}

    window_start = date.fromisoformat(cursor["window_start"]) if "window_start" in cursor else first_date
    today = datetime.utcnow().date()

    if window_start >= today:
        return [], cursor

    while True:
        width = window_days
        while True:
            window_end = min(window_start + timedelta(days=width), today)
            url = "/".join([
                BASE_URL, "jsonsummary", "canonical",
                instrument, "OBJECT", "science",
                f"{_date_str(window_start)}-{_date_str(window_end)}",
            ])
            resp = requests.get(url, cookies=cookies, timeout=120)
            if resp.status_code != 200 or not resp.headers.get("content-type", "").startswith("application/json"):
                raise RuntimeError(
                    f"GOA request failed (status {resp.status_code}) -- {COOKIE_ENV_VAR} is likely "
                    "missing or stale. Re-login at https://archive.gemini.edu and refresh the env var."
                )
            records_json = resp.json()
            if len(records_json) < RESPONSE_ROW_CAP:
                break
            if width <= 1:
                logger.warning(
                    "GOA returned %d rows for %s %s-%s even at the minimum 1-day window -- "
                    "genuinely hit the ~%d-row response cap on a single day's data. Proceeding "
                    "with whatever's in this response and moving past this day rather than "
                    "blocking the rest of the scan -- some of %s's data may be missing.",
                    len(records_json), instrument, _date_str(window_start), _date_str(window_end),
                    RESPONSE_ROW_CAP, _date_str(window_start),
                )
                break
            width = max(1, width // 2)

        rows = [r for r in records_json if is_reduced(r.get("filename", ""))]
        if rows or window_end >= today:
            break
        window_start = window_end

    records = []
    for r in rows:
        filename = r["filename"]
        ut_datetime = r.get("ut_datetime")
        obs_date = datetime.fromisoformat(ut_datetime.replace("Z", "+00:00")).date() if ut_datetime else None
        records.append(
            RawObservation(
                archive_obs_id=filename,
                archive_url=DOWNLOAD_URL.format(filename=filename),
                instrument=instrument,
                obs_date=obs_date,
                program_id=r.get("observation_id") or r.get("data_label"),
                raw_target_name=r.get("object"),
            )
        )

    return records, {"window_start": window_end.isoformat()}
