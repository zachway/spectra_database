"""NOIRLab Astro Data Archive (DECam/SOAR/Mayall).

Not implemented: the /tap endpoint 404'd during initial investigation and
the real access mechanism (REST API? a different TAP path?) was never
resolved.

Note: DESI does NOT depend on this — it turned out to be served directly by
data.desi.lbl.gov as a standalone VAC file (see sync.archives.desi), not
through NOIRLab Data Lab as originally assumed.
"""

from sync.base import RawObservation


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    raise NotImplementedError(
        "NOIRLab Data Lab: /tap 404'd and the real access mechanism was never "
        "resolved. Needs a fresh live investigation — check the current NOIRLab "
        "Astro Data Archive docs for the actual TAP/REST endpoint."
    )
