"""Gemini GHOST spectrograph — authenticated GOA JSON API (not CADC).

https://archive.gemini.edu/help/api.html

gemini.py (CADC/ivoa.ObsCore, dataproduct_type='spectrum') misses GHOST's
actual reduced spectra -- confirmed live on a real observation
(GS-2023A-SV-103-6-001, WD 1145+017, 2023-05-10): CADC's caom2.Plane only
carries 2 raw (calibrationLevel=1, dataProductType='image') planes for it,
but Gemini's own archive (GOA) has the real reduced products --
S20230510S0056_blue001_calibrated.fits, _red001_calibrated.fits, etc. --
that never made it into CADC's mirror at all. GOA is Gemini's primary/home
archive; CADC is an international partner mirror that's evidently missing
some of GHOST's per-arm calibrated products (GHOST splits light into
red/blue arms, hence the paired filenames). This module goes straight to
GOA instead of routing around CADC's gap.

Broader per-instrument check (caom2.Plane, calibrationLevel by instrument):
GHOST actually has some reduced (level 2) planes in CADC (2,143 of them,
against 21,278 raw) -- this specific observation's gap wasn't total, just
patchy. GMOS-N/GNIRS show the same small-but-present reduced fraction
(normal). GPI/IGRINS/MAROON-X show zero level-2 planes in CADC at all --
IGRINS confirmed live via GOA to have real reduced products GOA-side
(see gemini_igrins.py) that CADC is missing entirely, not just patchily;
MAROON-X confirmed live via GOA to have no reduced products even on GOA
itself (visitor instrument, own separate pipeline, not Gemini's DRAGONS) --
nothing to sync there via this approach.

Filters to filenames containing "_calibrated" -- confirmed live (via the
GOA web UI, GHOST/science search) as the naming pattern for genuinely
reduced, science-ready per-arm spectra, as opposed to _dragons.fits
(intermediate pipeline product) or the bare raw file. Not going by the
documented `reduction` JSON field instead because its exact values for
GHOST specifically weren't confirmed live (GOA blocks anonymous access
from every environment this was developed in -- see below) --
`_calibrated` is the one signal actually seen on a real result set.

AUTHENTICATION: GOA now blocks anonymous access entirely ("Your IP address
range or ISP has been the source of excessive or malicious requests...
anonymous access has been denied" -- confirmed live, applies to the JSON
API too, not just the search form). There's no API key, only a session
cookie (gemini_archive_session) obtained by logging into
https://archive.gemini.edu (ORCID login recommended) in a real browser and
reading the cookie value off the post-login page. Set it as the
GOA_SESSION_COOKIE env var before running this archive. No documented
expiration, but it's tied to a real login session, not a durable token --
expect to re-login and refresh it periodically. Since morgan is headless,
that means logging in from a machine with a browser and copying the value
over each time it goes stale, not something this module can automate.
Shared with gemini_igrins.py -- see sync/archives/_goa_common.py.

Paginated by date window like gemini.py, same reasoning: GOA's date-range
selection syntax (YYYYMMDD-YYYYMMDD) doesn't guarantee every window has
data, so an empty window is looped past internally rather than treated as
"archive exhausted" by sync.main's generic driver. WINDOW_DAYS=90 is a
guess (not sized against real GHOST row-density the way gemini.py's 7-day
window was) -- GHOST is Gemini's newest instrument with a much smaller
total history than the rest of Gemini's ~30-year archive, so a wide window
should still be a small page.

VERIFIED LIVE (2026-07-22): a real run against real GOA data, 15 pages,
totals {'name_matched': 7421, 'skipped': 3515, 'stars_added': 230} -- the
URL syntax, field names, and _calibrated filter all confirmed correct.
"""

from datetime import date

from sync.archives._goa_common import fetch_reduced
from sync.base import RawObservation

INSTRUMENT = "GHOST"

# GHOST saw first light in Dec 2022 / began System Verification in 2023 --
# not live-confirmed further back than the one SV observation this module
# was built to catch (2023-05-10), so this is a conservative round start
# date rather than a confirmed true minimum.
FIRST_DATE = date(2023, 1, 1)

WINDOW_DAYS = 90


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    return fetch_reduced(cursor, INSTRUMENT, FIRST_DATE, WINDOW_DAYS, lambda filename: "_calibrated" in filename)
