"""CFHT / CADC (ESPaDOnS, SPIRou) — TAP, no native Gaia column.

Previously verified: byte/time-bounded query limits observed (not a simple
row cap), and a landing-page deep link works. Not implemented: the raw
FITS/datalink URL pattern for an individual dataset was never confirmed —
only the landing page. Needs that resolved before this is useful as more
than a "click through and find it yourself" pointer.
"""

from sync.base import RawObservation


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    raise NotImplementedError(
        "CFHT/CADC: TAP access and byte/time-bounded query limits are known, "
        "and a landing-page deep link works, but the raw FITS/datalink URL "
        "pattern was never confirmed. Verify that live before implementing."
    )
