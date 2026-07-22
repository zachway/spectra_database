"""Shared authenticated-GOA fetch logic for gemini_ghost.py and gemini_igrins.py.

Both instruments need the same GOA_SESSION_COOKIE auth, the same jsonsummary
date-windowed pagination (looping past empty windows internally, same reason
as gemini.py's CADC-based fetch -- an empty window doesn't mean the archive
is exhausted), and the same clear-error-on-auth-failure behavior. Only the
instrument name, the earliest date, and the reduced-product filename filter
differ per instrument -- see gemini_ghost.py and gemini_igrins.py for those
and the live evidence behind each one's filter choice.
"""

import os
from datetime import date, datetime, timedelta
from typing import Callable

import requests

from sync.base import RawObservation

BASE_URL = "https://archive.gemini.edu"
DOWNLOAD_URL = BASE_URL + "/file/{filename}"

COOKIE_ENV_VAR = "GOA_SESSION_COOKIE"


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
        window_end = min(window_start + timedelta(days=window_days), today)
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
