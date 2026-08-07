"""IRTF/iSHELL (NASA Infrared Telescope Facility) — IRSA-hosted CAOM2 TAP.

Wikipedia's IRTF page lists iSHELL alongside SpeX as IRTF's other current
instrument, missed in the first pass over this archive (which only checked
SpeX). Same IRSA-hosted CAOM2 tables and shared fetch logic as irtf_spex.py
-- see sync/archives/_irtf_common.py and irtf_spex.py's own docstring for
the full four-way-split-archive writeup (applies identically here: this
covers only the 2016B-present IRSA-hosted portion, not the pre-2016B
Legacy Archive).

iSHELL-specific facts confirmed live, all matching SpeX's shape exactly:
28,126 rows, every one calibrationlevel=1 (raw, no reduced tier), the
join to exactly one info/text/html summary.html artifact per plane holds
1:1 (28,126 both sides), and target_name uses the same underscore-joined
catalog-name convention (cleaned by the same _clean_name helper). iSHELL
data starts 2017-02-02 per IRSA's own documentation (not independently
re-derived here) -- covered automatically by the same MJD-watermark
pagination as SpeX, no separate FIRST_DATE needed since the cursor starts
at 0 either way.
"""

from __future__ import annotations

from sync.archives import _irtf_common
from sync.base import RawObservation

INSTRUMENT_PATTERN = "iSHELL%"
INSTRUMENT = "iSHELL"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    return _irtf_common.fetch(cursor, INSTRUMENT_PATTERN, INSTRUMENT)
