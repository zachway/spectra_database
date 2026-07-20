"""LAMOST DR11 (LRS + MRS) — bulk file dump or SQL API, direct Gaia DR3 column.

Previously verified: gaia_source_id is a first-party column on both
`catalogue` (LRS, 97.6% populated) and `med_catalogue` (MRS, 95.9%
populated), cross-matched by LAMOST's own pipeline at 3". No hard row cap
found on either the bulk gzipped FITS/CSV dump or the SQL API (tested to 3M
rows). The site itself (lamost.org) is reachable.

Not implemented: the exact SQL API endpoint, auth requirements, and query
dialect weren't re-verified in this session, and the full bulk file (>1GB)
can't be pulled from this dev machine to inspect directly. Needs a live pass
against the current LAMOST DR11 API docs before writing real query code —
not worth guessing at a Chinese-survey-archive API contract from memory.
"""

from sync.base import RawObservation


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    raise NotImplementedError(
        "LAMOST: gaia_source_id column and no-hard-cap behavior are verified "
        "(see module docstring), but the exact SQL API endpoint/auth/dialect "
        "needs a fresh live check, and the bulk file is too large to download "
        "and inspect from this machine."
    )
