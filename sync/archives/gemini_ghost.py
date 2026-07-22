"""Gemini GHOST spectrograph — authenticated GOA JSON API (not CADC).

https://archive.gemini.edu/help/api.html

gemini.py (CADC/ivoa.ObsCore, dataproduct_type='spectrum') misses GHOST's
actual reduced spectra -- confirmed live on a real observation
(GS-2023A-SV-103-6-001, WD 1145+017, 2023-05-10): CADC's caom2.Plane only
carries 2 raw (calibrationLevel=1, dataProductType='image') planes for it,
but Gemini's own archive (GOA) has the real reduced products --
S20230510S0056_blue001_calibrated.fits, _red001_calibrated.fits, etc. --
that never made it into CADC's mirror at all. GOA is Gemini's primary/home
archive; CADC is an international partner mirror that's evidently missing
some of GHOST's per-arm calibrated products (GHOST splits light into
red/blue arms, hence the paired filenames). This module goes straight to
GOA instead of routing around CADC's gap.

Filters to filenames containing "_calibrated" -- confirmed live (via the
GOA web UI, GHOST/science search) as the naming pattern for genuinely
reduced, science-ready per-arm spectra, as opposed to _dragons.fits
(intermediate pipeline product) or the bare raw file. Not going by the
documented `reduction` JSON field instead because its exact values for
GHOST specifically weren't confirmed live (GOA blocks anonymous access
from every environment this was developed in -- see below) --
`_calibrated` is the one signal actually seen on a real result set.

AUTHENTICATION: GOA now blocks anonymous access entirely ("Your IP address
range or ISP has been the source of excessive or malicious requests...
anonymous access has been denied" -- confirmed live, applies to the JSON
API too, not just the search form). There's no API key, only a session
cookie (gemini_archive_session) obtained by logging into
https://archive.gemini.edu (ORCID login recommended) in a real browser and
reading the cookie value off the post-login page. Set it as the
GOA_SESSION_COOKIE env var before running this archive. No documented
expiration, but it's tied to a real login session, not a durable token --
expect to re-login and refresh it periodically. Since morgan is headless,
that means logging in from a machine with a browser and copying the value
over each time it goes stale, not something this module can automate.

NOT LIVE-TESTED end to end -- GOA's anonymous block made that impossible
from every environment available while writing this (dev machine and
morgan both got the same "anonymous access denied" response even for the
JSON endpoints). Built directly against archive.gemini.edu/help/api.html's
documented URL syntax and field names (filename, data_label,
observation_id, object, ut_datetime, release) instead. First real run
needs a human with a valid cookie watching it -- if the URL syntax,
field names, or the _calibrated filename convention turn out wrong, this
will need a follow-up fix informed by the actual response shape.

Paginated by date window like gemini.py, same reasoning: GOA's date-range
selection syntax (YYYYMMDD-YYYYMMDD) doesn't guarantee every window has
data, so an empty window is looped past internally rather than treated as
"archive exhausted" by sync.main's generic driver. WINDOW_DAYS=90 is a
guess (not sized against real GHOST row-density the way gemini.py's 7-day
window was) -- GHOST is Gemini's newest instrument with a much smaller
total history than the rest of Gemini's ~30-year archive, so a wide window
should still be a small page; revisit if a real run proves otherwise.
"""

import os
from datetime import date, datetime, timedelta

import requests

from sync.base import RawObservation

BASE_URL = "https://archive.gemini.edu"
DOWNLOAD_URL = BASE_URL + "/file/{filename}"

COOKIE_ENV_VAR = "GOA_SESSION_COOKIE"

INSTRUMENT = "GHOST"

# GHOST saw first light in Dec 2022 / began System Verification in 2023 --
# not live-confirmed further back than the one SV observation this module
# was built to catch (2023-05-10), so this is a conservative round start
# date rather than a confirmed true minimum.
FIRST_DATE = date(2023, 1, 1)

WINDOW_DAYS = 90


def _date_str(d: date) -> str:
    return d.strftime("%Y%m%d")


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    cookie = os.environ.get(COOKIE_ENV_VAR)
    if not cookie:
        raise RuntimeError(
            f"{COOKIE_ENV_VAR} not set. Log into {BASE_URL} (ORCID login recommended), "
            "copy the gemini_archive_session cookie value from the post-login page, and "
            f"set {COOKIE_ENV_VAR} to it before running the gemini_ghost archive."
        )
    cookies = {"gemini_archive_session": cookie}

    window_start = date.fromisoformat(cursor["window_start"]) if "window_start" in cursor else FIRST_DATE
    today = datetime.utcnow().date()

    if window_start >= today:
        return [], cursor

    while True:
        window_end = min(window_start + timedelta(days=WINDOW_DAYS), today)
        url = "/".join([
            BASE_URL, "jsonsummary", "canonical",
            INSTRUMENT, "OBJECT", "science",
            f"{_date_str(window_start)}-{_date_str(window_end)}",
        ])
        resp = requests.get(url, cookies=cookies, timeout=120)
        if resp.status_code != 200 or not resp.headers.get("content-type", "").startswith("application/json"):
            raise RuntimeError(
                f"GOA request failed (status {resp.status_code}) -- {COOKIE_ENV_VAR} is likely "
                "missing or stale. Re-login at https://archive.gemini.edu and refresh the env var."
            )
        records_json = resp.json()

        rows = [r for r in records_json if "_calibrated" in r.get("filename", "")]
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
                instrument=INSTRUMENT,
                obs_date=obs_date,
                program_id=r.get("observation_id") or r.get("data_label"),
                raw_target_name=r.get("object"),
            )
        )

    return records, {"window_start": window_end.isoformat()}
