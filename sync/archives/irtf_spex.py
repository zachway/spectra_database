"""IRTF/SpeX (NASA Infrared Telescope Facility) — IRSA-hosted CAOM2 TAP.

The user's tip that this archive "is split into a few different databases"
is confirmed live, and matters for scope: IRTF's public holdings are spread
across at least four independent systems --

1. IRTF Legacy Archive (irtfdata.ifa.hawaii.edu) -- 2000B-2016A semesters,
   pure HTML directory browsing + a web search form, no API found.
2. This module's source: the IRSA-hosted IRTF Data Archive
   (irsa.ipac.caltech.edu) -- 2016B-present (SpeX from 2016-08-02 onward,
   confirmed live), the only piece with a real machine-readable API (IBE +
   TAP, both mirroring the same underlying CAOM2 tables).
3. irtf.mearth_spectra -- a separate, unrelated IRSA table ("IRTF MEarth
   Spectra") sitting right next to the CAOM ones in the same IBE mission
   listing; a genuinely different dataset, not covered by this module.
4. The IRTF Spectral Library (irtfweb.ifa.hawaii.edu/~spex/IRTF_Spectral_Library/)
   -- a curated, static reference library of ~2000 published stellar/
   substellar spectra, not an observation-log archive with a query API.

This module only covers #2, which is also the only one with new data still
arriving. Confirmed live via IRSA's TAP_SCHEMA that the CAOM2 model here is
split the same way as CADC's (see dao.py/cfht_cadc.py): caom.observation_irtf
(instrument/target/proposal) joined to caom.plane_irtf (calibration level,
time bounds) via obsid, plus caom.artifact_irtf (actual file URIs) via
planeid -- IRSA's IBE-listed "irtf_tables" page only advertises the plane
table, but the observation/artifact tables exist and are queryable directly
once you know the CADC-style naming convention.

NO POSITION DATA AT ALL for SpeX, confirmed live: every one of 86,511
SpeX planes has a NULL observation.targetposition_coordinates (checked with
a live COUNT), and the plane table itself carries no s_ra/s_dec-equivalent
columns either (only internal spatial index descriptors). Name-only
matching, same shape as lick.py -- ra/dec left None throughout, matcher
falls through straight to name_resolved and silently skips anything that
doesn't hit a tracked alias (see sync/matcher.py's third path).

target_name needs light cleanup before it's usable as an identifier,
confirmed live against real recent rows: catalog names come through
underscore-joined ("HD_218251", "SAO_74912", "TIC_404752671") -- matcher's
_normalize_name already strips whitespace before comparing, so converting
"_" to " " is enough to line these up with SIMBAD-style aliases. Some also
carry an appended reddening estimate ("HD_176542_AV=+1.16", confirmed live
on ~750 of 86,511 rows) that must be stripped first or the name never
matches anything. No filtering of non-stellar rows (target_name=="calibration",
~16,847 rows; minor planet designations like "623 Chimaera (A907 BC)";
Landolt standard fields like "SA 93-101") -- none of these will ever match a
tracked star's alias list, so they fall through to a harmless skip exactly
like any bulk archive's non-target rows elsewhere in this project.

calibrationlevel is 1 (raw) on every single SpeX row, confirmed live (no
row at any other level) -- this IRSA mirror only carries IRTF's raw output,
there's no separate reduced-product tier to pick between (unlike
gemini_igrins.py's GOA gap). reduction_status_from_calib_level still used
for consistency/future-proofing rather than hardcoding "raw", in case IRSA
ever ingests a calibrated tier.

archive_url points at each plane's real summary.html landing page (e.g.
".../summary/2022B013/sbd_20220802_060505/summary.html"), pulled directly
from caom.artifact_irtf (producttype='info', contenttype='text/html') --
confirmed live to exist as exactly one such row per plane (a plain page a
human can open, listing the observation's raw frames/weather/QA), not a
raw signed-nothing FITS link. Deliberately not the science FITS artifacts
themselves: each plane bundles many of those per sequence (flats, arcs, and
the actual target frames all as separate 'science'-producttype artifacts,
confirmed live -- e.g. 8 raw frames under one plane), so there's no single
canonical file to point at the way DataLink resolvers give CADC-based
archives one.

Paginated via a plain MJD watermark on plane.time_bounds_lower, same shape
as dao.py. PAGE_SIZE=5000 chosen generously under volume (86,511 total
rows confirmed live) with no sign of a cliff -- a 20,000-row version of
this exact join query ran in under 5s live.
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
WHERE o.instrument_name LIKE 'SpeX%' AND a.producttype = 'info' AND a.contenttype = 'text/html'
  AND p.time_bounds_lower > {last_mjd}
ORDER BY p.time_bounds_lower ASC
"""

PAGE_SIZE = 5000

INSTRUMENT = "SpeX"

# Strips a trailing reddening annotation like "_AV=+1.16" or "_AV=-0.4" —
# confirmed live on real target_name values (see module docstring).
_AV_SUFFIX = re.compile(r"_AV=[+-]?\d+\.?\d*$")


def _clean_name(raw: str) -> str:
    return _AV_SUFFIX.sub("", raw).replace("_", " ").strip()


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_mjd = cursor.get("last_mjd", 0)

    tap = make_tap_service(TAP_URL)
    query = QUERY.format(page_size=PAGE_SIZE, last_mjd=last_mjd)
    table = tap.search(query, maxrec=PAGE_SIZE).to_table()

    records = []
    max_mjd = last_mjd
    for row in table:
        mjd = float(row["time_bounds_lower"])
        max_mjd = max(max_mjd, mjd)
        raw_name = str(row["target_name"])
        records.append(
            RawObservation(
                archive_obs_id=str(row["planeid"]),
                archive_url=str(row["uri"]),
                instrument=INSTRUMENT,
                obs_date=Time(mjd, format="mjd").to_datetime().date(),
                program_id=str(row["proposal_id"]),
                raw_target_name=_clean_name(raw_name) if raw_name else None,
                reduction_status=reduction_status_from_calib_level(row["calibrationlevel"]),
            )
        )

    new_cursor = {"last_mjd": max_mjd if records else last_mjd}
    return records, new_cursor
