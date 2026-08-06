"""Las Cumbres Observatory / FLOYDS spectrograph — public Science Archive REST API.

The user's link (archive.lco.global's web UI) is the front end for a real,
documented, unauthenticated REST API at archive-api.lco.global -- confirmed
live, no API key needed for public data (LCO's "Portal username/password"
auth is only required for proprietary data or a higher daily rate limit).
LCO does have a spectrograph, in fact two: FLOYDS (low-res, long-slit, one
per site pair -- instrument codes en06/en12) and NRES (high-res echelle,
fl0X-coded, network of 4 dedicated fiber-fed units). This module only
covers FLOYDS -- confirmed live via OBSTYPE=SPECTRUM that every one of its
52,038 public spectrum frames is an "en"-coded FLOYDS frame; NRES appears
to use a different obstype/configuration_type value entirely and would need
its own separate investigation (not done here).

Reduction tiers, confirmed live via RLEVEL counts on OBSTYPE=SPECTRUM:
0 = raw (33,784), 90 = legacy IRAF/ORAC-reduced (804, deprecated pipeline),
91 = BANZAI-FLOYDS-reduced 1D spectrum (16,960; basename suffix "-e91-1d"
confirms these are genuinely extracted 1D products, not raw 2D frames --
became LCO's default pipeline output as of 2025-08-04 per LCO's own
documentation, but live data shows real e91 products going back to at
least 2021-03-01, i.e. already retroactively reprocessed well before that
switchover). Same "duplicate rows for one physical exposure" problem as
gemini_igrins.py's spec_a0v.fits choice -- multiple RLEVELs of the same
underlying exposure are separate `frame` records here, not versions of one
record -- so this module picks exactly one tier (RLEVEL=91) rather than
unioning all three, same reasoning as that module.

NO POSITION DATA on spectrum frames, confirmed live: unlike LCO's imaging
frames (which carry a GeoJSON `area` sky-footprint polygon), a real e91
FLOYDS record has no `area` key at all. Name-only matching, same shape as
irtf_spex.py/lick.py -- ra/dec left None, matcher falls straight through
to name_resolved. target_name comes through already SIMBAD-shaped on real
rows (e.g. "GD153", "FEIGE110", "BD+284211", "L745-46A") -- matcher's
_normalize_name strips whitespace before comparing, so no cleanup needed.
No filtering by proposal_id (spectrophotometric-standard proposals like
"FLOYDS standards"/"COJ_calib" show up in the sample above) -- those are
still real, nameable stars, harmless to pass through like any archive's
calibration-target rows elsewhere in this project.

archive_url is the API's own stable per-frame resolver
(archive-api.lco.global/frames/{id}/), not the `url` field the search
response embeds directly -- confirmed live that field is a *pre-signed* S3
URL (X-Amz-Expires=172800, i.e. good for only 48 hours), which would go
dead in the database days after being stored. The resolver endpoint
returns a fresh signed URL on every request instead.

Pagination: confirmed live that anonymous requests cap `limit` at exactly
100 (150+ -> HTTP 400 "Large limit not allowed for anonymous users";
100 -> 200 OK) -- PAGE_SIZE below reflects that hard ceiling, not a
guess. Watermarked on observation_date, same shape as dao.py's t_min, but
with one real wrinkle confirmed live: the API's `start=` filter is
*inclusive* (a frame whose observation_date exactly equals `start` is
returned again), and there's no working `__gt`-style strict-inequality
lookup (`observation_date__gt=...` is silently ignored, confirmed live --
returns the unfiltered count). A naive "set cursor = max date seen" would
re-fetch and re-count the same boundary record(s) forever once the archive
is caught up, since `sum(counts.values())` would never hit zero. Guarded
against by tracking the frame ids seen at the current watermark alongside
the date, and dropping any row that's both at that exact date *and* one of
those already-seen ids before deciding whether the page was actually empty.
"""

from __future__ import annotations

from datetime import date, datetime

import requests

from sync.base import RawObservation

BASE_URL = "https://archive-api.lco.global/frames/"

INSTRUMENT = "FLOYDS"

RLEVEL = 91

# Confirmed live: anonymous requests reject limit > 100 outright (HTTP 400).
PAGE_SIZE = 100

EPOCH = "2000-01-01T00:00:00.000000Z"


def _resolver_url(frame_id: int) -> str:
    return f"{BASE_URL}{frame_id}/"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_date = cursor.get("last_date", EPOCH)
    last_ids = set(cursor.get("last_ids", []))

    resp = requests.get(
        BASE_URL,
        params={
            "public": "true",
            "OBSTYPE": "SPECTRUM",
            "RLEVEL": RLEVEL,
            "ordering": "observation_date",
            "limit": PAGE_SIZE,
            "start": last_date,
        },
        timeout=(15, 60),
    )
    resp.raise_for_status()
    results = resp.json()["results"]

    records = []
    max_date = last_date
    max_date_ids: set[int] = set(last_ids)
    for row in results:
        frame_id = int(row["id"])
        obs_date_str = row["observation_date"]
        if obs_date_str == last_date and frame_id in last_ids:
            continue

        if obs_date_str > max_date:
            max_date = obs_date_str
            max_date_ids = set()
        if obs_date_str == max_date:
            max_date_ids.add(frame_id)

        obs_date: date = datetime.strptime(row["observation_day"], "%Y-%m-%d").date()
        records.append(
            RawObservation(
                archive_obs_id=str(frame_id),
                archive_url=_resolver_url(frame_id),
                instrument=INSTRUMENT,
                obs_date=obs_date,
                program_id=row.get("proposal_id"),
                raw_target_name=row.get("target_name") or None,
                reduction_status="reduced",
            )
        )

    new_cursor = {"last_date": max_date, "last_ids": sorted(max_date_ids)}
    return records, new_cursor
