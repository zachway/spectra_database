"""4MOST — confirmed empty archive (no public data yet).

Once live, this rides the existing ESO TAP integration (see eso.py) rather
than needing its own access-pattern investigation — 4MOST is an ESO
instrument and will show up in ivoa.ObsCore the same way.
"""

from sync.base import RawObservation


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    raise NotImplementedError(
        "4MOST: archive confirmed empty. Once live, extend eso.py's query rather "
        "than building a separate fetch — 4MOST data lives in the same ESO "
        "ObsCore table."
    )
