"""Las Cumbres Observatory / NRES spectrograph — public Science Archive REST API.

The instruments page (lco.global/observatory/instruments) lists this as
LCO's second spectrograph, missed in the first pass over this archive —
Network of Robotic Echelle Spectrographs, high-res, fiber-fed, one per 1m
site. See sync/archives/_lco_common.py for the shared fetch/grouping/
pagination logic and its full live-investigation writeup.

NRES-specific facts: identified by OBSTYPE=TARGET, not SPECTRUM (confirmed
live — SPECTRUM is FLOYDS-only) — TARGET also covers plain Sinistro imaging
on the same "fa"-prefixed instrument codes NRES happens to share (the
spectrograph reads out through a repurposed Sinistro-family detector), so
OBSTYPE is the real discriminator here, not instrument_id. Real reduced
fraction is much lower than FLOYDS's, confirmed live: only 1,034 of 73,200
raw TARGET frames have reached a BANZAI-NRES RLEVEL=92 product at all — the
per-block grouping in _lco_common (raw-only groups fall back to their raw
frame) is what keeps this module from silently dropping the ~98% of real
NRES observations that haven't been reprocessed. Per LCO's own instruments
page, NRES "will not be offered as of semester 2026B" — actively winding
down, but its ~15 years of historical public spectra remain fully in scope.

Unlike FLOYDS, NRES frames carry a real GeoJSON `area` sky-footprint
polygon (confirmed live) — ra/dec is populated from its centroid, so this
gets a real positional-match fallback, not name-only. The footprint itself
is small (tens of arcsec across on the one block checked live) but not
tiny enough to trust much past matcher.py's default 1" easy-match radius
on its own — positional matching here is a genuine but imprecise backup,
identifier match is still tried first as always.
"""

from __future__ import annotations

from sync.archives import _lco_common
from sync.base import RawObservation

OBSTYPE = "TARGET"
INSTRUMENT = "NRES"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    return _lco_common.fetch(cursor, OBSTYPE, INSTRUMENT)
