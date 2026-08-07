"""Shared IRSA-hosted IRTF (CAOM2/TAP) fetch logic for irtf_spex.py and irtf_ishell.py.

Both of IRTF's current instruments (SpeX and iSHELL) live in the exact same
IRSA-hosted CAOM2 tables, differing only in `instrument_name` -- confirmed
live: iSHELL's shape matches SpeX's in every respect checked (28,126 rows,
every one calibrationlevel=1/raw, the join to exactly one info/text/html
summary.html artifact per plane holds 1:1 same as SpeX's, target_name uses
the same underscore-joined catalog-name convention). See irtf_spex.py's
original docstring (still the fuller writeup) for the four-way-split
archive landscape, the no-position-data finding, and the summary.html
archive_url reasoning -- all apply identically here.
"""

from __future__ import annotations

import re

from astropy.time import Time

from sync.base import RawObservation, make_tap_service, reduction_status_from_calib_level

TAP_URL = "https://irsa.ipac.caltech.edu/TAP"

QUERY = """
SELECT TOP {page_size} o.target_name, o.proposal_id, p.time_bounds_lower, p.planeid, p.calibrationlevel, a.uri
FROM caom.observation_irtf o
JOIN caom.plane_irtf p ON o.obsid = p.obsid
JOIN caom.artifact_irtf a ON a.planeid = p.planeid
WHERE o.instrument_name LIKE '{instrument_pattern}' AND a.producttype = 'info' AND a.contenttype = 'text/html'
  AND p.time_bounds_lower > {last_mjd}
ORDER BY p.time_bounds_lower ASC
"""

PAGE_SIZE = 5000

# Strips a trailing reddening annotation like "_AV=+1.16" or "_AV=-0.4" —
# confirmed live on real SpeX target_name values (see irtf_spex.py).
_AV_SUFFIX = re.compile(r"_AV=[+-]?\d+\.?\d*$")


def _clean_name(raw: str) -> str:
    return _AV_SUFFIX.sub("", raw).replace("_", " ").strip()


def fetch(cursor: dict, instrument_pattern: str, instrument: str) -> tuple[list[RawObservation], dict]:
    last_mjd = cursor.get("last_mjd", 0)
    last_planeids = set(cursor.get("last_planeids", []))

    tap = make_tap_service(TAP_URL)
    query = QUERY.format(page_size=PAGE_SIZE, instrument_pattern=instrument_pattern, last_mjd=last_mjd)
    table = tap.search(query, maxrec=PAGE_SIZE).to_table()

    records = []
    max_mjd = last_mjd
    max_mjd_planeids: set[str] = set(last_planeids)
    for row in table:
        planeid = str(row["planeid"])
        mjd = float(row["time_bounds_lower"])
        # IRSA's TAP output rounds time_bounds_lower to 6 decimals for
        # display (confirmed live via the field's own irsa_format: "12.6f"
        # metadata), but the server-side WHERE clause compares against the
        # full-precision stored value -- so a boundary row's true value can
        # sit just above the rounded watermark we send back, re-matching
        # `> {last_mjd}` on the very next page (confirmed live: exactly one
        # repeated planeid at the page boundary). Same same-timestamp
        # id-dedup guard as lco_floyds.py/lco_nres.py's inclusive `start=`
        # workaround, for a different root cause but the same symptom.
        if mjd == last_mjd and planeid in last_planeids:
            continue

        if mjd > max_mjd:
            max_mjd = mjd
            max_mjd_planeids = set()
        if mjd == max_mjd:
            max_mjd_planeids.add(planeid)

        raw_name = str(row["target_name"])
        records.append(
            RawObservation(
                archive_obs_id=planeid,
                archive_url=str(row["uri"]),
                instrument=instrument,
                obs_date=Time(mjd, format="mjd").to_datetime().date(),
                program_id=str(row["proposal_id"]),
                raw_target_name=_clean_name(raw_name) if raw_name else None,
                reduction_status=reduction_status_from_calib_level(row["calibrationlevel"]),
            )
        )

    new_cursor = {"last_mjd": max_mjd, "last_planeids": sorted(max_mjd_planeids)}
    return records, new_cursor
