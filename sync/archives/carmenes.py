"""CARMENES — three resources, none Gaia-matched, none fully implementable yet.

Verified live: the GTO DR1 portal (362 stars, 2016-2020) is a fixed table of
per-star download links (SERVAL-format epochs bundled per star, not one file
per observation), and the TAC template library (382 stars) is one co-added
FITS per star per arm. Neither carries a Gaia column — would need a
Karmn/SIMBAD-name -> Gaia positional crosswalk, same idea as
sdss_legacy_optical.py's approach but for a much smaller, static star list.

Not implemented: DR1's per-star zips would need to be downloaded and
unzipped to enumerate individual epoch dates (each zip holds multiple
epochs) — real work, not just a query, and not worth it before the
Karmn->Gaia crosswalk exists. The broader CAHA archive (all CARMENES
programs, incl. post-2020) is a JS-rendered SPA whose TAP/SIAP claims were
never confirmed live.
"""

from sync.base import RawObservation


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    raise NotImplementedError(
        "CARMENES: no Gaia column on any of the three resources — needs a "
        "Karmn/name -> Gaia crosswalk built first (see module docstring). DR1 "
        "also requires downloading and unzipping per-star archives to get "
        "individual epoch dates, which is real implementation work, not just a "
        "query. CAHA's TAP/SIAP claims are still unverified."
    )
