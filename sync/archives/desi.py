"""DESI — has a direct Gaia DR3 source_id column (desi_dr1.mws.source_id),
but is hosted on NOIRLab Data Lab, whose access mechanism isn't resolved.
Blocked on sync.archives.noirlab, not a separate investigation.
"""

from sync.base import RawObservation


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    raise NotImplementedError(
        "DESI: query would be simple (desi_dr1.mws.source_id is a direct Gaia "
        "join) once NOIRLab Data Lab's access mechanism is resolved — see "
        "sync.archives.noirlab. Nothing DESI-specific left to figure out."
    )
