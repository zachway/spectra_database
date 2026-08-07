"""Las Cumbres Observatory / FLOYDS spectrograph — public Science Archive REST API.

See sync/archives/_lco_common.py for the shared fetch/grouping/pagination
logic and its full live-investigation writeup. FLOYDS-specific facts:
low-res long-slit, one unit per 2m site (en06/en12), fully identified by
OBSTYPE=SPECTRUM (confirmed live: every one of 52,038 public spectrum
frames is en-coded). Position availability depends on which reduction
tier ends up representing a given observation, not fixed per-instrument —
see _lco_common's docstring.
"""

from __future__ import annotations

from sync.archives import _lco_common
from sync.base import RawObservation

OBSTYPE = "SPECTRUM"
INSTRUMENT = "FLOYDS"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    return _lco_common.fetch(cursor, OBSTYPE, INSTRUMENT)
