"""Shared archive-api.lco.global fetch logic for lco_floyds.py and lco_nres.py.

Both instruments turned out to need real per-observation grouping, not a
single-RLEVEL-tier pick like gemini_igrins.py's -- confirmed live, the
naive "just filter to the best RLEVEL" approach both undercounts (most
real observations, of either instrument, only have a raw RLEVEL=0 frame at
any given moment -- BANZAI-FLOYDS/BANZAI-NRES reprocessing lags behind, and
a real chunk of raw exposures never gets reprocessed at all) and, if
widened to include every RLEVEL, way overcounts: an unfiltered OBSTYPE=
SPECTRUM sample surfaced RLEVEL values like 14/16/17/23/24/25/27/28/35/36/
67/73/98 alongside 0/90/91 -- confirmed live these are all internal
engineering/commissioning frames (proposal_id == 'calibrate' on every one
sampled, old IRAF-stage-letter basenames like "wecfzst_..._ws_1", and a
literal bogus 1973 observation_date shared by many of them), not real
reduction tiers a user would ever want. NRES's own calibrate-proposal
frames are dated normally (confirmed live, starting 2020-03-01) -- the
bogus-1973 issue is FLOYDS-specific, but the proposal_id == 'calibrate'
exclusion handles both. Only the exact literal 'calibrate' is excluded,
not any proposal_id containing "calib" as a substring -- 'FLOYDS standards'
and 'COJ_calib' are real per-star spectrophotometric-standard proposals
(GD153, Feige110, BD+28 4211, ...), confirmed live to produce normal,
real-star observations that belong in the output like any other target.

Real grouping key, confirmed live: `observation_id` (aka BLKUID) ties
together every frame from one LCO "block" -- but a block can span far more
than one instrument's reduction tiers. Querying one real block unfiltered
(observation_id=825858139) returned 155 frames mixing FLOYDS science
frames with an sd05 guide-camera's entire frame stream; the OBSTYPE filter
has to be applied *before* grouping, not skipped in favor of grouping
alone, or a block collapses across totally unrelated instruments. Once
scoped to one obstype, a block's members are a real one-physical-target
processing family: e.g. a real NRES block held a raw `-e00` frame plus
`-e92-1d` (1D extracted spectrum, the one wanted), `-e92-2d` (rectified 2D,
a diagnostic), and `-e92-summary` (a plot/report) all under the same
RLEVEL=92 -- so even RLEVEL alone doesn't uniquely pick the right member;
the `-1d` basename suffix is the real "this is the final science spectrum"
signal (same role gemini_igrins.py's `spec_a0v.fits` substring plays).
FLOYDS additionally has a second legitimate final-tier shape, RLEVEL=90
under a plain (non-"-1d"-suffixed) "FLOYDSstandards_..." basename --
confirmed live on real, current (2026-08) per-night standard-star
exposures, not a deprecated pipeline artifact as first assumed from an
initial (wrong, too-narrow) RLEVEL=90-count-only check.

_rank below picks, per group: any `-1d`-suffixed member first (best);
else RLEVEL 90 (FLOYDS's other real final tier); else whatever's left
(normally RLEVEL 0, raw) -- covering every group, including thousands of
current real observations that only have a raw frame right now, without
ever emitting more than one row for one physical block. archive_obs_id is
the block id itself (not the per-frame id), so a later sync run that finds
a block's reduction has since caught up simply UPSERTs that same row from
raw to reduced (ON CONFLICT DO UPDATE, see sync/matcher.py) instead of
creating a second row -- this also self-heals the case where a block's raw
and reduced frames land on different fetch() pages.

Position data is per-*frame*, not per-instrument -- confirmed live, a raw
(RLEVEL=0) or RLEVEL=90 FLOYDS/NRES frame carries a real GeoJSON `area`
sky-footprint polygon (still image-shaped, has WCS), but the final `-1d`
1D-extracted product does not (the spatial axis is gone by then, confirmed
live on a real e91-1d/e92-1d record: no `area` key at all). ra/dec is the
simple centroid of `best`'s own polygon corners when present, None
otherwise -- so whichever tier `_rank` actually picked determines whether
a given group gets a positional fallback or falls through to name-only;
this is not a fixed per-instrument property the way it first looked from
checking only one `-1d` record.

Some very old/manually-triggered engineering frames carry a null
`observation_id` (confirmed live, 2017-era ENG-proposal NRES frames with no
proposal_id and no target_name at all) -- grouping on that key blindly
would silently coalesce multiple *unrelated* observations into a single
`archive_obs_id='None'` row, discarding all but whichever one `_rank`
happened to prefer (a real bug caught live via the pagination-overlap
smoke test: two different frames on two different pages both produced
`archive_obs_id='None'`, registering as a false "duplicate" between pages).
Rows with `observation_id is None` are skipped entirely instead --
consistent with skipping any other archive's malformed/non-target rows.

Pagination: anonymous requests cap `limit` at exactly 100 (confirmed live,
150+ -> HTTP 400) and the `start=` filter is inclusive with no working
strict-inequality lookup (confirmed live, `observation_date__gt=...` is
silently ignored) -- same client-side same-timestamp id-dedup boundary
guard as the original lco_floyds.py. Filtering out proposal_id=='calibrate'
happens client-side, after the request -- a page that's *entirely*
'calibrate' noise must not look like "caught up" to sync.runner (which
stops once a fetch() call returns zero records), so this loops internally,
advancing the watermark across successive upstream pages, until it either
produces at least one real group or the upstream API itself runs out of
rows -- confirmed live this doesn't take more than a handful of extra
pages in practice (a full ascending page starting from the very first
'calibrate' frame already reaches 2021 by its 100th row), but MAX_INNER_PAGES
below still bounds it defensively rather than trusting that forever.
"""

from __future__ import annotations

import logging
from datetime import date, datetime

import requests

from sync.base import RawObservation

logger = logging.getLogger(__name__)

BASE_URL = "https://archive-api.lco.global/frames/"

# Confirmed live: anonymous requests reject limit > 100 outright (HTTP 400).
PAGE_SIZE = 100

EPOCH = "2000-01-01T00:00:00.000000Z"

# Internal engineering/commissioning frames, not real observations of a
# star -- see module docstring. Exact match only.
INTERNAL_PROPOSAL_ID = "calibrate"

# Defensive bound on how many upstream pages one fetch() call will walk
# past while looking for at least one non-'calibrate' group -- see module
# docstring for why an all-noise page must not look like "caught up".
MAX_INNER_PAGES = 300


def _resolver_url(frame_id: int) -> str:
    return f"{BASE_URL}{frame_id}/"


def _rank(frame: dict) -> tuple:
    if frame["basename"].endswith("-1d"):
        tier = 0
    elif frame["reduction_level"] == 90:
        tier = 1
    else:
        tier = 2
    return (tier, -frame["reduction_level"], frame["id"])


def _is_reduced(frame: dict) -> bool:
    return frame["basename"].endswith("-1d") or frame["reduction_level"] == 90


def _centroid(area: dict | None) -> tuple[float, float] | None:
    if not area:
        return None
    coords = area["coordinates"][0]
    points = coords[:-1] if coords[0] == coords[-1] else coords
    if not points:
        return None
    lon = sum(p[0] for p in points) / len(points)
    lat = sum(p[1] for p in points) / len(points)
    return lon, lat


def _fetch_one_page(obstype: str, last_date: str) -> list[dict]:
    resp = requests.get(
        BASE_URL,
        params={
            "public": "true",
            "OBSTYPE": obstype,
            "ordering": "observation_date",
            "limit": PAGE_SIZE,
            "start": last_date,
        },
        timeout=(15, 60),
    )
    resp.raise_for_status()
    return resp.json()["results"]


def fetch(cursor: dict, obstype: str, instrument: str) -> tuple[list[RawObservation], dict]:
    last_date = cursor.get("last_date", EPOCH)
    last_ids = set(cursor.get("last_ids", []))

    groups: dict[int, list[dict]] = {}
    for _ in range(MAX_INNER_PAGES):
        results = _fetch_one_page(obstype, last_date)
        if not results:
            break

        max_date = last_date
        max_date_ids: set[int] = set(last_ids)
        for row in results:
            frame_id = int(row["id"])
            obs_date_str = row["observation_date"]
            if obs_date_str == last_date and frame_id in last_ids:
                continue
            if row["proposal_id"] != INTERNAL_PROPOSAL_ID and row["observation_id"] is not None:
                groups.setdefault(row["observation_id"], []).append(row)
            if obs_date_str > max_date:
                max_date = obs_date_str
                max_date_ids = set()
            if obs_date_str == max_date:
                max_date_ids.add(frame_id)

        last_date, last_ids = max_date, max_date_ids
        if groups:
            break
    else:
        logger.warning("%s: hit MAX_INNER_PAGES (%d) still looking for real data", instrument, MAX_INNER_PAGES)

    records = []
    for blkuid, members in groups.items():
        best = min(members, key=_rank)
        centroid = _centroid(best.get("area"))
        ra, dec = centroid if centroid else (None, None)
        obs_date: date = datetime.strptime(best["observation_day"], "%Y-%m-%d").date()
        records.append(
            RawObservation(
                archive_obs_id=str(blkuid),
                archive_url=_resolver_url(best["id"]),
                instrument=instrument,
                obs_date=obs_date,
                program_id=best.get("proposal_id"),
                ra=ra,
                dec=dec,
                raw_target_name=best.get("target_name") or None,
                reduction_status="reduced" if _is_reduced(best) else "raw",
            )
        )

    new_cursor = {"last_date": last_date, "last_ids": sorted(last_ids)}
    return records, new_cursor
