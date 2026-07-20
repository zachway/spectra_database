"""Keck Observatory Archive — TAP, no native Gaia column (positional match).

Previously verified: `ORDER BY` + `TOP` returned unsorted results in
testing — a real bug, not a documentation gap. Any pagination here must use
WHERE-range chunking (e.g. a date/mjd watermark, like eso.py and
sdss_legacy_optical.py) rather than server-side sort+limit.

Not implemented: the exact table/column names and the deep-link URL pattern
were never live-verified in this session — only the sort+limit bug was
previously confirmed. Follows the same shape as eso.py once those are
checked.
"""

from sync.base import RawObservation


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    raise NotImplementedError(
        "KOA: known ORDER BY + TOP bug means pagination must use WHERE-range "
        "chunking, not server-side sort+limit — but the actual table/column "
        "names and deep-link pattern were never live-verified. Check the "
        "current KOA TAP schema before writing real query code."
    )
